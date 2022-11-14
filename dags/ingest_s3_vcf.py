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

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2))
def ingest_s3_vcf():
    """
    ### Ingest VCF files from S3
    """
    # [END instantiate_dag]

    list_s3_files = S3ListOperator(
        task_id='list_s3_files',
        bucket='synthetic-gvcfs',
        prefix='gvcfs/',
        delimiter='/',
        aws_conn_id='aws'
    )



# [START dag_invocation]
ingest_s3_vcf = ingest_s3_vcf()
# [END dag_invocation]
