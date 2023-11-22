import re
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from google.cloud.exceptions import NotFound

# Change the BigQuery project ID, dataset ID, and table ID according to your project details
# No need to create a table before it will create automcatically, if it exists it won't create
project_id = 'etl-streaming-402314'
dataset_id = 'pubsub_dataset'
table_id = 'puzzle_cleaned_v6'

# Initialize BigQuery client
bigquery_client = bigquery.Client(project=project_id)

# Read your CSV file into a DataFrame and give the path to your file location

puzzle_csv = pd.read_csv('/path to file/puzzle_prepocess/puzzleSample.csv')

# Define the schema of your BigQuery table
schema = [
    bigquery.SchemaField("documentType", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("customerId", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("documentId", "STRING", mode="NULLABLE"),

    bigquery.SchemaField("Ultimate Parent", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Ultimate Parent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Immediate Parent", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Immediate Parent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Subject", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Subject", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Subsidiary", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Subsidiary", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("Sibling", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("Sibling", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("document_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    ]),
]

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
        "Ultimate Parent": r'\{Ultimate Parent,\s([^,]+),\s((?:{business,[^}]+(?:experian|edw)},\s*)+)([^,]+,\s[^,]+,\s[^,]+)',
        "Immediate Parent": r'\{Immediate Parent,\s([^,]+),\s((?:{business,[^}]+(?:experian|edw)},\s*)+)([^,]+,\s[^,]+,\s[^,]+)',
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

            extracted_data.append({
                company_type: company_name,
                'document_id': doc_id.strip().strip(','),
                'address': address.replace(', null', '').replace('null', '')
            })
        if not extracted_data:
            extracted_data.append({
                company_type: None,
                'document_id': None,
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
        "Ultimate Parent": [],
        "Immediate Parent": [],
        "Subject": [],
        "Subsidiary": [],
        "Sibling": [],
    }

    for relation_type, data_list in row['children_data'].items():
        document_data[relation_type].extend([
            {
                relation_type: data[relation_type],
                "document_id": data["document_id"],
                "address": data["address"]
            } for data in data_list
        ])

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