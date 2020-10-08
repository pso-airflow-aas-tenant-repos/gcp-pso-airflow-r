# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""DAG Quality tests

Tests DAG Syntax, compatibility with environment and load time."""

import time

import airflow.settings
import pytest
from airflow.models import DagBag
from sfdc_airflow.cluster_policy import cluster_policy

LOAD_SECOND_THRESHOLD = 2


@pytest.fixture(name="dagbag")
def dagbag_fixture(monkeypatch):
    """Setup dagbag for each test."""
    monkeypatch.setattr(airflow.settings, "policy",
                        cluster_policy)
    return DagBag(
        dag_folder='./dags/',
        include_examples=False)


def test_import_dags(dagbag):
    """Tests there are no syntax issues or environment compaibility issues.
    """
    errs = dagbag.import_errors
    assert len(errs) == 0, f'DAG import failures: {errs}'


def test_import_time(dagbag):
    """Test that all DAGs can be parsed under the threshold time."""
    for dag_id in dagbag.dag_ids:
        start = time.time()

        filepath = dagbag.get_dag(dag_id).filepath
        dagbag.process_file(filepath)

        end = time.time()
        total = end - start
        assert total < LOAD_SECOND_THRESHOLD, f'parsing {filepath} was too slow'
