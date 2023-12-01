import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils import trigger_rule

# Define environment-specific configuration dictionaries
ENV_CONFIG = {
    "nonlive": {
        "project_id": "vf-uk-nwp-nonlive",
        "bucket_name": "vf-uk-nwp-nonlive-prp-beacon",
        "base_path_prefix": "vf-uk-nwp-nonlive-prp-beacon/puzzle_code/",
        "env_specifier": "preprod",
    },
    "live": {
        "project_id": "vf-uk-nwp-nonlive",
        "bucket_name": "vf-uk-nwp-nonlive-qa",
        "base_path_prefix": "vf-uk-nwp-nonlive-qa/puzzle_data/",
        "env_specifier": "prod",
    },
}

# Retrieve the name of the environment from the airflow variables
project = os.environ.get("GCP_PROJECT")
if "nonlive" in project:
    environment = "nonlive"
else:
    environment = "live"

print('project_name ',project)

print('environment ',environment)

# Retrieve environment-specific values from the configuration dictionary
env_config = ENV_CONFIG.get(environment, {})

PROJECT_ID = env_config.get("project_id", "")
BUCKET_NAME = env_config.get("bucket_name", "")
SPARK_SCRIPT_PATH = "puzzle_code/puzzle_preprocess_pyspark_v1.py"
ENV_SPECIFIER = env_config.get("env_specifier", "")

default_dag_args = {
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5),
}

PYSPARK_JOB = {
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/{SPARK_SCRIPT_PATH}",
        "args": [environment],
    },
    "reference": {"project_id": PROJECT_ID},
    "placement": {
        "cluster_name": "quantexa-puzzle-cluster"
    },
}


default_dag_args = {
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5),
}

with models.DAG(
    "puzzle_preprocess_pyspark_v1",
    schedule_interval=None,
    default_args=default_dag_args,
    concurrency=1,
    description="Load Puzzle CSV Data then transform children column data to structured data from GCS to BigQuery",
    start_date=datetime.datetime(2023, 4, 13),
    catchup=False,
) as dag:
    # Override cluster creation to enable gateway component
    class CustomDataprocClusterCreateOperator(
        dataproc_operator.DataprocClusterCreateOperator
    ):
        def __init__(self, *args, **kwargs):
            super(CustomDataprocClusterCreateOperator, self).__init__(*args, **kwargs)

        def _build_cluster_data(self):
            cluster_data = super(
                CustomDataprocClusterCreateOperator, self
            )._build_cluster_data()
            cluster_data["config"]["endpointConfig"] = {"enableHttpPortAccess": True}
            cluster_data["config"]["softwareConfig"]["optionalComponents"] = ["JUPYTER"]
            return cluster_data

    # Create features creation Dataproc cluster.
    create_cluster_task = CustomDataprocClusterCreateOperator(
        task_id="create-cluster",
        project_id=PROJECT_ID,
        cluster_name="quantexa-puzzle-cluster",
        enable_component_gateway=True,
        storage_bucket=BUCKET_NAME,
        region="europe-west2",
        service_account=f"vf-uk-nwp-prp-beacon-dp-ops-sa@vf-uk-nwp-nonlive.iam.gserviceaccount.com",
        subnetwork_uri=f"projects/vf-uk-nwp-nonlive/regions/europe-west2/subnetworks/prp-beacon-restricted-zone",
        tags=['allow-internal-dataproc-prp-beacon', 'allow-ssh-from-management-zone-prp-beacon'],
        master_machine_type="n2-standard-8",
        master_disk_type="pd-ssd",
        master_disk_size=500,
        num_workers=2,
        worker_machine_type="n2-standard-8",
        worker_disk_type="pd-ssd",
        worker_disk_size=500,
        num_preemptible_workers=2,
        image_version="2.1-debian11",
        idle_delete_ttl=3600,
        internal_ip_only=True,
        metadata=[("enable-oslogin", "true")],
        properties={
            "core:fs.gs.implicit.dir.repair.enable": "false",
            "core:fs.gs.status.parallel.enable": "true",
            "yarn:yarn.nodemanager.resource.memory-mb": "80000",
            "yarn:yarn.nodemanager.resource.cpu-vcores": "16",
            "yarn:yarn.nodemanager.pmem-check-enabled": "false",
            "yarn:yarn.nodemanager.vmem-check-enabled": "false",
            "dataproc:am.primary_only": "true",
            "spark:spark.yarn.am.cores": "3",
            "spark:spark.yarn.am.memory": "10g",
            "spark:spark.yarn.am.memoryOverhead": "2g",
            "spark:spark.shuffle.service.enabled": "true",
            "spark:spark.jars": f"gs://{BUCKET_NAME}/spark-jars/spark-bigquery-with-dependencies_2.12-0.28.0.jar",
        },
    )

    # Run PySpark job in cluster.
    job_task = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        job=PYSPARK_JOB,
        region="europe-west2",
        project_id=PROJECT_ID,
    )


    # Delete Cloud Dataproc cluster, using XCom to retrieve the cluster name
    delete_cluster_task = dataproc_operator.DataprocClusterDeleteOperator(
        task_id="delete_dataproc_cluster",
        cluster_name="quantexa-puzzle-cluster",
        region="europe-west2",
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    )

    # Define DAG dependencies.
    create_cluster_task >> job_task >> delete_cluster_task