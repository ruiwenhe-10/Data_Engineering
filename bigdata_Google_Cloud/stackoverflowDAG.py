import os

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)
# from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "utopian-pact-288603")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "composer-spark-stackoverflow")
REGION = os.environ.get("GCP_LOCATION", "us-west3")
ZONE = os.environ.get("GCP_REGION", "us-west3-a")
# BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "dataproc-system-tests")
# OUTPUT_FOLDER = "wordcount"
# OUTPUT_PATH = "gs://{}/{}/".format(BUCKET, OUTPUT_FOLDER)
# PYSPARK_MAIN = os.environ.get("PYSPARK_MAIN", "hello_world.py")
# PYSPARK_URI = "gs://{}/{}".format(BUCKET, PYSPARK_MAIN)
# SPARKR_MAIN = os.environ.get("SPARKR_MAIN", "hello_world.R")
# SPARKR_URI = "gs://{}/{}".format(BUCKET, SPARKR_MAIN)


# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]

# Update options
# [START how_to_cloud_dataproc_updatemask_cluster_operator]
CLUSTER_UPDATE = {
    "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 3}}
}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}
# [END how_to_cloud_dataproc_updatemask_cluster_operator]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://ruiwen_test_storage/bigdata/bigdata-stackoverflow.jar"],
        "main_class": "stackoverflow.StackOverflow",
    },
}

with models.DAG("gcp_dataproc_spark", start_date=days_ago(1), schedule_interval=None) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    spark_task = DataprocSubmitJobOperator(
    	task_id="spark_task", job=SPARK_JOB, location=REGION, project_id=PROJECT_ID
    )

    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]
    

    create_cluster >> spark_task >> delete_cluster





