#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# pylint: disable=missing-function-docstring

# [START tutorial]
# [START import_module]
import json
import re
import os
import tiledb, tiledbvcf

from airflow import XComArg
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

dag_params = {
    's3_bucket': 'synthetic-gvcfs',
    's3_prefix': 'gvcfs/1000',
    's3_conn_id': 'aws',
    'tiledb_array_uri': 's3://tiledb-nikos/arrays/test-vcf'
}
# [END default_args]

# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), params=dag_params)
def s3_vcf_to_tiledb():
    """
    ### Ingest VCF files from S3
    """
    # [END instantiate_dag]

    s3_files = S3ListOperator(
        task_id='list_s3_files',
        bucket="{{ params.s3_bucket }}",
        prefix="{{ params.s3_prefix }}",
        delimiter='/',
        aws_conn_id='aws',
        params=dag_params,
    )

    def split(a_list, chunk_size):
        for i in range(0, len(a_list), chunk_size):
            yield a_list[i:i + chunk_size]

    @task
    def partition_files(files, chunk_size):
        p = re.compile('^.*\.bcf$')
        bcf_files = [ f"s3://synthetic-gvcfs/{s}" for s in files if p.match(s)]
        return list(split(bcf_files, chunk_size))

    @task
    def create_array(array_uri):
        aws_hook = AwsBaseHook(aws_conn_id="aws")
        credentials = aws_hook.get_credentials()
        tiledb_config = tiledb.Config()
        tiledb_config.set('vfs.s3.aws_access_key_id', credentials.access_key)
        tiledb_config.set('vfs.s3.aws_secret_access_key', credentials.secret_key)
        cfg = tiledbvcf.ReadConfig(tiledb_config=tiledb_config)
        tiledbvcf.config_logging("info")
        ds = tiledbvcf.Dataset(array_uri, mode="w", cfg=cfg, stats=True)
        ds.create_dataset()

    @task
    def ingest_vcf_to_tiledb(files, array_uri):
        aws_hook = AwsBaseHook(aws_conn_id="aws")
        credentials = aws_hook.get_credentials()
        tiledb_config = tiledb.Config()
        tiledb_config.set('vfs.s3.aws_access_key_id', credentials.access_key)
        tiledb_config.set('vfs.s3.aws_secret_access_key', credentials.secret_key)
        cfg = tiledbvcf.ReadConfig(tiledb_config=tiledb_config)
        tiledbvcf.config_logging("info")
        print(f"tiledbvcf v{tiledbvcf.version}")
        print(f"Ingesting into array {array_uri}")
        ds = tiledbvcf.Dataset(array_uri, mode="w", cfg=cfg, stats=True)
        print(f"Opened {array_uri} (schema v{ds.schema_version()})")
        ds.ingest_samples(
            sample_uris=files,
            total_memory_budget_mb=2048,
            threads=4
        )

    create_array(array_uri=dag_params.get('tiledb_array_uri'))
    partitions = partition_files(XComArg(s3_files), 2)
    ingest_vcf_to_tiledb.partial(array_uri=dag_params.get('tiledb_array_uri')).expand(files=partitions)

# [START dag_invocation]
s3_vcf_to_tiledb = s3_vcf_to_tiledb()
# [END dag_invocation]
