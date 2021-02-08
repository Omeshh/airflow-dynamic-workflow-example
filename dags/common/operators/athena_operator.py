# -*- coding: utf-8 -*-
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
#

from uuid import uuid4

from airflow.contrib.hooks.aws_athena_hook import AWSAthenaHook
from airflow.contrib.sensors.aws_athena_sensor import AthenaSensor
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AWSAthenaOperator(BaseOperator):
    """
    An operator that submit presto query to athena.
    :param query: Presto to be run on athena. (templated)
    :type query: str
    :param database: Database to select. (templated)
    :type database: str
    :param output_location: s3 path to write the query results into. (templated)
    :type output_location: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    :param sleep_time: Time to wait between two consecutive call to check query status on athena
    :type sleep_time: int
    """

    ui_color = '#44b5e2'
    template_fields = ('query', 'database', 'output_location')

    @apply_defaults
    def __init__(self, query, database, output_location, aws_conn_id='aws_default', client_request_token=None,
                 query_execution_context=None, result_configuration=None, sleep_time=30, *args, **kwargs):
        super(AWSAthenaOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.database = database
        self.output_location = output_location
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.query_execution_id = None
        self.hook = None

    def get_hook(self):
        return AWSAthenaHook(self.aws_conn_id, self.sleep_time)

    def execute(self, context):
        """
        Run Presto Query on Athena
        """
        self.hook = self.get_hook()
        self.hook.get_conn()

        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location
        self.query_execution_id = self.hook.run_query(self.query, self.query_execution_context,
                                                      self.result_configuration, self.client_request_token)
        query_status = self.hook.poll_query_status(self.query_execution_id)

        # changed from original operator to check status from poll and add to xcom (opened issue with Apache)

        context['task_instance'].xcom_push(key='query_execution_id', value=self.query_execution_id)
        context['task_instance'].xcom_push(key='query_status', value=query_status)

        if query_status not in AthenaSensor.SUCCESS_STATES:
            raise AirflowException('Athena poll query failed {0} {1}'
                                   .format(query_status, self.output_location + self.query_execution_id + '.log'))
