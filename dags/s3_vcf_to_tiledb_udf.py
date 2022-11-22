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

from airflow import XComArg
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import get_current_context

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
    'tiledb_array_uri': 's3://tiledb-nikos/arrays/test-vcf',
    'tiledb_worker_memory': Param(2048, type="integer", minimum=1024),
    'tiledb_worker_threads': Param(4, type="integer", minimum=1),
    'vcf_files_per_worker': Param(2, type="integer", minimum=1),
    'tiledb_namespace': 'TileDB-Inc',
    'tiledb_token': ''
}
# [END default_args]

# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), params=dag_params)
def s3_vcf_to_tiledb_udf():
    """
    ### Ingest VCF files from S3
    """
    # [END instantiate_dag]

    s3_files = S3ListOperator(
        task_id='list_s3_files',
        bucket="{{ params.s3_bucket }}",
        prefix="{{ params.s3_prefix }}",
        delimiter='/',
        aws_conn_id="aws",
        params=dag_params,
    )

    def split_list(a_list, chunk_size):
        for i in range(0, len(a_list), chunk_size):
            yield a_list[i:i + chunk_size]

    @task
    def partition_files(files, chunk_size):
        context = get_current_context()
        p = re.compile('^.*\.bcf$')
        s3_bucket = context["params"]["s3_bucket"]
        bcf_files = [ f"s3://{s3_bucket}/{s}" for s in files if p.match(s)]
        return list(split_list(bcf_files, int(chunk_size)))

    @task
    def create_array(array_uri):
        import tiledb, tiledbvcf
        context = get_current_context()
        aws_hook = AwsBaseHook(aws_conn_id=context["params"]["s3_conn_id"])
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
        import tiledb, tiledbvcf, tiledb.cloud
        from tiledb.cloud import udf
        context = get_current_context()
        aws_hook = AwsBaseHook(aws_conn_id=context["params"]["s3_conn_id"])
        credentials = aws_hook.get_credentials()
        tiledb_token = context["params"]["tiledb_token"]
        tiledb.cloud.login(tiledb_token)
        tiledb_config = {'vfs.s3.aws_access_key_id': credentials.access_key,
                         'vfs.s3.aws_secret_access_key': credentials.secret_key,
                         'vfs.s3.connect_timeout_ms': 120000,
                         'vfs.s3.request_timeout_ms': 120000}
        tiledb.cloud.udf.exec(
            func = "adam-wenocur/ingest_vcf_samples",
            array_uri = array_uri,
            contig = "chr1",
            sample_uris = files,
            partition_idx_count = [0, 1],
            tiledb_config = tiledb_config,
            memory_budget_mb = context["params"]["tiledb_worker_memory"],
            threads = context["params"]["tiledb_worker_threads"],
            stats = True,
            namespace=context["params"]["tiledb_namespace"]
        )

    create_array(array_uri="{{ params.tiledb_array_uri }}")
    partitions = partition_files(XComArg(s3_files), "{{ params.vcf_files_per_worker }}")
    ingest_vcf_to_tiledb.partial(array_uri="{{ params.tiledb_array_uri }}").expand(files=partitions)

# [START dag_invocation]
s3_vcf_to_tiledb_udf = s3_vcf_to_tiledb_udf()
# [END dag_invocation]
