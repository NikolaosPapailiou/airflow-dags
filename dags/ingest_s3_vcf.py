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

from airflow import XComArg
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

dag_params = {
    's3_bucket': 'synthetic-gvcfs',
    's3_prefix': 'gvcfs/',
    's3_conn_id': 'aws',
    'tiledb_token': ''
}
# [END default_args]

# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), params=dag_params)
def ingest_s3_vcf():
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
        bcf_files = [s for s in files if p.match(s)]
        return list(split(bcf_files, chunk_size))

    ingest_vcf_to_tiledb = KubernetesPodOperator.partial(
        namespace='airflow',
        image="703933321414.dkr.ecr.us-east-1.amazonaws.com/tiledb-airflow:latest",
        cmds=["bash", "-cx"],
        name="ingest_vcf_to_tiledb",
        is_delete_operator_pod=True,
        in_cluster=True,
        task_id="ingest_vcf_to_tiledb",
        do_xcom_push=True,
    )

    partitions = partition_files(XComArg(s3_files), 10)
    ingest_vcf_to_tiledb.expand(arguments=["mkdir -p /airflow/xcom/;", "echo", partitions, ">", "/airflow/xcom/return.json"])

# [START dag_invocation]
ingest_s3_vcf = ingest_s3_vcf()
# [END dag_invocation]
