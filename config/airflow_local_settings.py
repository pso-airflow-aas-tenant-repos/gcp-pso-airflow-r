from sfdc_airflow.cluster_policy import cluster_policy


def policy(task):
    """Cluster policy"""
    cluster_policy(task)
