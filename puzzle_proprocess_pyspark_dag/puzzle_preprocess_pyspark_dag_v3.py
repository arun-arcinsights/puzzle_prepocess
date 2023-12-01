import datetime
import os
import re
import yaml
from zipfile import ZipFile

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils import trigger_rule



def get_file_from_location(filename):
    zipfile, post_zip = re.search(r'(.*\.zip)?(.*)', filename).groups()
    if zipfile:
        return ZipFile(zipfile).read(post_zip.lstrip('/'))
    else:
        return open(post_zip).read()


def load_yaml():
    yaml_file_name = 'puzzle_preprocess_pyspark_config.yaml'
    directory_path = os.path.dirname(__file__)
    yaml_path = os.path.join(directory_path, "resources/{}".format(yaml_file_name))

    yaml_file = get_file_from_location(yaml_path)
    data = yaml.safe_load(yaml_file)

    return data


# YAML info
env = models.Variable.get('environment').strip()
data = load_yaml()

compute_project_id = data[env]['compute_project_id']
datahub_project_id = data[env]['datahub_project_id']
spark_output_bucket = data[env]['spark_output_bucket']
service_account = data[env]['service_account']
main_python_file_uri=data[env]['main_python_file_uri']
subnetwork_uri = data[env]['subnetwork_uri']
subnet_tags=data[env]['subnet_tags']
spark_jar = data[env]['spark_jar']

# Spark job variables
bq_datasetid = data[env]['bq_datasetid']
bq_table = data[env]['bq_table']
csv_files_path = data[env]['csv_files_path']


print('project_name ',compute_project_id)

print('environment ',env)


default_dag_args = {
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=5),
}

PYSPARK_JOB = {
    "pyspark_job": {
        "main_python_file_uri": f"gs://{compute_project_id}/{main_python_file_uri}",
        "args": [compute_project_id,bq_datasetid,bq_table,csv_files_path],
    },
    "reference": {"project_id": compute_project_id,},
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
    "puzzle_preprocess_pyspark_v2",
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
        project_id=compute_project_id,
        cluster_name="quantexa-puzzle-cluster",
        enable_component_gateway=True,
        storage_bucket=spark_output_bucket,
        region="europe-west2",
        service_account=service_account,
        subnetwork_uri=subnetwork_uri,
        tags=subnet_tags,
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
            "spark:spark.jars": spark_jar,
        },
    )

    # Run PySpark job in cluster.
    job_task = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        job=PYSPARK_JOB,
        region="europe-west2",
        project_id=compute_project_id,
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