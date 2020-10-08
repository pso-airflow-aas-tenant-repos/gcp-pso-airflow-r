# Airflow as a Service Tenant Repository
This repository provides a template for individual tenants to use to deploy DAGs to an Airflow-aaS.

# Getting Started
```
curl https://pyenv.run | bash
pyenv virtualenv 3.7.8 airflow
pyenv activate airflow
pip install -r requirements.txt
pre-commit install
```

# Understanding the Pre-commit
The pre-commit is intended to push CI checks like linting code into
the developers workflow to decrease back and forth during code review.
These checks will run on all staged files when every you run `git commit`
The checks run are configured in `.pre-commit-config.yaml`.
[Learn more about the pre-commit project here](http://pre-commit.com/)
Notably, the pre-commit will run pylint with the
[`pylint-airflow`](https://github.com/BasPH/pylint-airflow) plugin
to catch common airflow mistakes before commiting them to the codebase.

# Running the Tests
The unittests can be easily run with the [pytest](https://docs.pytest.org/en/stable/).
From the root of the repo simply run:
```
pytest
```

# Managing Airflow Variables
[Airflow Variables](https://airflow.apache.org/docs/stable/concepts.html#variables)
should be committed to the codebase in [`config/variables.json`](config/variables.json).
These values will be picked up by the git-sync process and available on your
airflow infrastructure.

# DAG Deployment Flow
![DAG Deployment Flow](https://airflow.apache.org/docs/stable/concepts.html#variables)
In order to deploy DAGs you must follow the following steps:
1. Author your DAG (and any associated tests) locally
1. Pass pre-commit locally
1. Open Pull Request (this will run the CI tests as github action to verify you complied with steps 2 & 3)
1. Recieve at least 1 approving code review from a teammate.
1. Merge the PR.

From there, an asynchronous git-sync process which constantly runs on the airflow deployment infrastructure
will pull the latest changes from the repository into a volume mounted by the webserver and workers.
Your DAGs will appear in the airlfow UI and will be picked up by the scheduler automatically within minutes.

# Using `sfdc-airflow-aas`
`sfdc-airflow-aas` is a python package maintained by the Airflow-aaS team.
It will contain reusable modules for common Airflow patterns used at SFDC supported by the service.

## Cluster Policies
Notably, the `sfdc_cluster_policy` module contains the [Airflow Cluster Policies](https://airflow.apache.org/docs/1.10.10/concepts.html#cluster-policy)
that will be deployed in your Airflow infrastructure.
The Airflow-aaS team will use these policies to assert that DAGs and tasks comply to certain contraints that protect the service.

For more information on these constraints you can refer to [`sfdc_cluster_policy/rules.py`](https://github.com/PolideaInternal/pso-google-sfdc-airflow-aas/blob/develop/sfdc-airflow-aas/sfdc_cluster_policy/rules.py)
