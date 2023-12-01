from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery, storage
import pandas as pd
import re
from google.cloud.exceptions import NotFound

# Define your BigQuery project ID, dataset ID, and table ID
project_id = 'vf-uk-nwp-nonlive'
dataset_id = 'vfuk_dh_lake_all_rawprepared_s'
table_id = 'puzzle_cleaned_v3'

# Initialize BigQuery client
bigquery_client = bigquery.Client(project=project_id)

# Define the schema of your BigQuery table
schema = [
    bigquery.SchemaField("documentType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("customerId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("documentId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Ultimate_Parent", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Ultimate_Parent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("entityId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("ParentId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Immediate_Parent", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Immediate_Parent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("entityId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("ParentId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Subject", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Subject", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("entityId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("ParentId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Subsidiary", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Subsidiary", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("entityId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("ParentId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Sibling", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Sibling", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("entityId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("ParentId", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("docType", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
]

# Define the DAG
dag = DAG(
    'puzzle_preprocess_v2',
    default_args={
        'owner': 'Arun',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Read data from GCS and preprocess the puzzle children column and write cleaned data to BigQuery',
    schedule_interval=None,
)

def run_puzzle_processing(**kwargs):
    # Read your CSV file into a DataFrame and give the path to your file location
    puzzle_csv = pd.read_csv('gs://vf-uk-nwp-nonlive-qa/puzzle_data/puzzleSample.csv')

    # Check if the table exists
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    try:
        bigquery_client.get_table(table_ref)
        print(f"Table {table_ref.table_id} already exists.")
    except NotFound:
        # If the table doesn't exist, create it
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)  # Make an API request.
        print(f"Created table {table.table_id}")

    def extract_information_from_csv(row):
        # Patterns to extract information
        patterns = {
            "Ultimate_Parent": r'\{Ultimate Parent,\s([^,]+),\s((?:{business,[^}]+(?:experian|edw)},\s*)+)([^,]+,\s[^,]+,\s[^,]+)',
            "Immediate_Parent": r'\{Immediate Parent,\s([^,]+),\s((?:{business,[^}]+(?:experian|edw)},\s*)+)([^,]+,\s[^,]+,\s[^,]+)',
            "Subject": r'\{Subject,\s([^,]+),\s((?:{business,[^}]+(?:experian|edw)},\s*)+)([^,]+,\s[^,]+,\s[^,]+)',
            "Subsidiary": r'\{Subsidiary,\s([^,]+),\s((?:{business,[^}]+(?:experian|edw)},\s*)+)([^,]+,\s[^,]+,\s[^,]+)',
            "Sibling": r'\{Sibling,\s([^,]+),\s((?:{business,[^}]+(?:experian|edw)},\s*)+)([^,]+,\s[^,]+,\s[^,]+)'
        }

        # Function to extract information using the given pattern
        def extract_information(input_str, pattern, company_type):
            matches = re.finditer(pattern, input_str)
            extracted_data = []

            for match in matches:
                company_name, doc_id, address = match.groups()

                # Extract information for entityId and ParentId
                entity_pattern = r'{([^,]+),\s([^,]+),\s([^,]+)}'
                entity_match = re.search(entity_pattern, doc_id.strip().strip(','))

                parent_pattern = r',\s([^,]+),\s([^,]+),\s([^,]+)}'
                parent_match = re.search(parent_pattern, doc_id.strip().strip(','))

                if entity_match:
                    entity_type, entity_value, entity_docType = entity_match.groups()
                    entity_type=entity_type.strip().strip('{')
                    entity_docType=entity_docType.strip().strip('{').strip('}')
                else:
                    entity_type = entity_value = entity_docType = None

                if parent_match:
                    parent_type, parent_value, parent_docType = parent_match.groups()
                    parent_type=parent_type.strip().strip('{').strip('}')
                    parent_docType=parent_docType.strip().strip('{').strip('}')
                else:
                    parent_type = parent_value = parent_docType = None

                extracted_data.append({
                    company_type: company_name,
                    'entityId': {
                        'type': entity_type,
                        'value': entity_value,
                        'docType': entity_docType
                    },
                    'ParentId': {
                        'type': parent_type,
                        'value': parent_value,
                        'docType': parent_docType
                    },
                    'address': address.replace(', null', '').replace('null', '').strip().strip(',').strip()
                })

            if not extracted_data:
                extracted_data.append({
                    company_type: None,
                    'entityId': {
                        'type': None,
                        'value': None,
                        'docType': None
                    },
                    'ParentId': {
                        'type': None,
                        'value': None,
                        'docType': None
                    },
                    'address': None
                })

            return extracted_data

        # Extract information for each relation using the corresponding pattern
        data = {}
        for relation_type, pattern in patterns.items():
            data[relation_type] = extract_information(row['children'], pattern, relation_type)

        # Return the extracted data
        return data

    # Apply the UDF to each row in the DataFrame
    puzzle_csv['children_data'] = puzzle_csv.apply(extract_information_from_csv, axis=1)
    rows_to_insert = []

    for _, row in puzzle_csv.iterrows():
        document_data = {
            "documentType": row["documentType"],
            "customerId": row["customerId"],
            "documentId": row["documentId"],
            "Ultimate_Parent": [],
            "Immediate_Parent": [],
            "Subject": [],
            "Subsidiary": [],
            "Sibling": [],
        }

        for relation_type, data_list in row['children_data'].items():
            document_data[relation_type].extend(data_list)

        rows_to_insert.append(document_data)

    # Print the first document_data
    print(len(rows_to_insert))

    # Insert rows into BigQuery table
    errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)
        
    if errors:
        print(f"Errors encountered while inserting rows: {errors}")
    else:
        print("Rows inserted successfully.")


print("Data written to BigQuery table.")

# Define the PythonOperator task and set the task to run your logic
run_puzzle_processing_task = PythonOperator(
    task_id='run_puzzle_processing_task',
    python_callable=run_puzzle_processing,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
# For example, you can set run_puzzle_processing_task to run after another_task
run_puzzle_processing_task
