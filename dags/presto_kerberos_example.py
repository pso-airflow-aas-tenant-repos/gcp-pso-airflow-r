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
import os
from datetime import timedelta
from distutils.util import strtobool

import prestodb
from airflow import DAG, AirflowException
from airflow.configuration import conf
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from prestodb.exceptions import DatabaseError
from prestodb.transaction import IsolationLevel


class PrestoException(Exception):
    """
    Presto exception
    """


def _boolify(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return strtobool(value)
    return value


class PrestoHook(DbApiHook):
    """
    Interact with Presto through prestodb.
    >>> ph = PrestoHook()
    >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
    >>> ph.get_records(sql)
    [[340698]]
    """

    conn_name_attr = "presto_conn_id"
    default_conn_name = "presto_default"

    def get_conn(self):
        """Returns a connection object"""
        db_conn = self.get_connection(self.presto_conn_id)  # pylint: disable=no-member
        extra = db_conn.extra_dejson
        auth = None
        if db_conn.password and extra.get("auth") == "kerberos":
            raise AirflowException("Kerberos authorization doesn't support password.")
        if db_conn.password:
            auth = prestodb.auth.BasicAuthentication(db_conn.login, db_conn.password)
        elif extra.get("auth") == "kerberos":
            auth = prestodb.auth.KerberosAuthentication(
                config=extra.get("kerberos__config", os.environ.get("KRB5_CONFIG")),
                service_name=extra.get("kerberos__service_name"),
                mutual_authentication=_boolify(
                    extra.get("kerberos__mutual_authentication", False)
                ),
                force_preemptive=_boolify(
                    extra.get("kerberos__force_preemptive", False)
                ),
                hostname_override=extra.get("kerberos__hostname_override"),
                sanitize_mutual_error_response=_boolify(
                    extra.get("kerberos__sanitize_mutual_error_response", True)
                ),
                principal=extra.get(
                    "kerberos__principal", conf.get("kerberos", "principal")
                ),
                delegate=_boolify(extra.get("kerberos__delegate", False)),
                ca_bundle=extra.get("kerberos__ca_bundle"),
            )

        presto_conn = prestodb.dbapi.connect(
            host=db_conn.host,
            port=db_conn.port,
            user=db_conn.login,
            source=db_conn.extra_dejson.get("source", "airflow"),
            http_scheme=db_conn.extra_dejson.get("protocol", "http"),
            catalog=db_conn.extra_dejson.get("catalog", "hive"),
            schema=db_conn.schema,
            auth=auth,
            isolation_level=self.get_isolation_level(),
        )
        if extra.get("verify") is not None:
            # Unfortunately verify parameter is available via public API.
            # The PR is merged in the presto library, but has not been released.
            # See: https://github.com/prestosql/presto-python-client/pull/31
            presto_conn._http_session.verify = _boolify(  # pylint: disable=protected-access
                extra["verify"]
            )

        return presto_conn

    def get_isolation_level(self):
        """Returns an isolation level"""
        db_conn = self.get_connection(self.presto_conn_id)  # pylint: disable=no-member
        isolation_level = db_conn.extra_dejson.get("isolation_level", "AUTOCOMMIT").upper()
        return getattr(IsolationLevel, isolation_level, IsolationLevel.AUTOCOMMIT)

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(";")

    def get_records(self, hql, parameters=None):
        """
        Get a set of records from Presto
        """
        try:
            return super().get_records(self._strip_sql(hql), parameters)
        except DatabaseError as err:
            raise PrestoException(err)

    def get_first(self, hql, parameters=None):
        """
        Returns only the first row, regardless of how many rows the query
        returns.
        """
        try:
            return super().get_first(self._strip_sql(hql), parameters)
        except DatabaseError as err:
            raise PrestoException(err)

    def get_pandas_df(self, hql, parameters=None, **kwargs):
        """
        Get a pandas dataframe from a sql query.
        """
        import pandas  # pylint: disable=import-outside-toplevel

        cursor = self.get_cursor()
        try:
            cursor.execute(self._strip_sql(hql), parameters)
            data = cursor.fetchall()
        except DatabaseError as err:
            raise PrestoException(err)
        column_descriptions = cursor.description
        if data:
            dataframe = pandas.DataFrame(data, **kwargs)
            dataframe.columns = [c[0] for c in column_descriptions]
        else:
            dataframe = pandas.DataFrame(**kwargs)
        return dataframe

    def run(self, hql, parameters=None):
        """
        Execute the statement against Presto. Can be used to create views.
        """
        return super().run(self._strip_sql(hql), parameters)

    def insert_rows(self, table, rows, target_fields=None, commit_every=0):
        """
        A generic way to insert a set of tuples into a table.
        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        """
        if self.get_isolation_level() == IsolationLevel.AUTOCOMMIT:
            self.log.info(
                "Transactions are not enable in presto connection. "
                "Please use the isolation_level property to enable it. "
                "Falling back to insert all rows in one transaction."
            )
            commit_every = 0

        super().insert_rows(table, rows, target_fields, commit_every)


def example_task():
    hook = PrestoHook()
    sql = "SELECT name FROM tpch.sf1.customer ORDER BY custkey ASC LIMIT 3"
    records = hook.get_records(sql)
    __import__("pprint").pprint(records)

    expected_records = [
        ["Customer#000000001"],
        ["Customer#000000002"],
        ["Customer#000000003"],
    ]

    if records != expected_records:
        raise AirflowException("Incorrect results")


args = {
    "owner": "jake",
}

dag = DAG(
    dag_id="sfdc_presto_kerberos",
    default_args=args,
    schedule_interval="*/15 * * * *",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=["sfdc"],
)

with dag:
    run_this = PythonOperator(
        task_id="example_task",
        python_callable=example_task,
        dag=dag,
    )
