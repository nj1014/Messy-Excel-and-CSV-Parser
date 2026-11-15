#These are my goals with this project:
#
#Parses multiple files without any error
#Creates and validates schema and omits empty columns
#Hashes PII
#Accessible by AI (which has safety checks and serve a specific role) using the RAG architecture

import pandas as pd
import os
import json
import hashlib
from collections import defaultdict
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage
import functions_framework

parsed_data = "/parsed_Data/"
bq = bigquery.Client()
pantry = storage.Client()
messy_data = "/spreadsheets/"
table = "rugged-coil-514202-s2.messydatatesting123"

job_config = bigquery.LoadJobConfig(schema_update_options=[
    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION]
    )

def upload(folder):
    print(folder.items())
    for item_parent, item in folder.items():
        for folder_name, file in item:
            folder_name = folder_name.replace("/", "-")
            table_name = table+"."+folder_name
            for col in file.select_dtypes(include=["object"], exclude=["str"]).columns:
                # Check the actual types inside the object column (ignoring empty NaN cells)
                inferred = pd.api.types.infer_dtype(file[col], skipna=True)
                
                # Only convert if the column has mixed types (e.g., strings + ints)
                if "mixed" in inferred:
                    file[col] = file[col].astype("string")
            bq.load_table_from_dataframe(file, table_name, job_config=job_config).result()

def read_data(bucket_name, blob_name):
    bucket = pantry.bucket(bucket_name) #Retrieves and iterates the contents in the bucket.
    blobby = bucket.list_blobs(prefix=blob_name)
    for file in blobby:
        if (not file.name.endswith("/") and not file.name.endswith(".DS_Store") and not file.name.endswith("audit_ground_truth.csv")):
            folder = defaultdict(list) #Kind of like a hashmap, i mean it's built on a hashmap so it's O(1) efficiency.
            format = ""
            with file.open('r') as f:
                if (file.name.endswith(".xlsx") or file.name.endswith(".xls")):
                    folder[str(Path(file.name).parent)].append((Path(file.name).stem, pd.read_excel(file.open("rb"), comment="#")))
                elif (file.name.endswith(".csv")):
                    folder[str(Path(file.name).parent)].append((Path(file.name).stem, pd.read_csv(file.open("rb"), comment="#", sep=None, engine="python")))
            upload(folder)


stringgg = """@functions_framework.cloud_event
def new_file(cloud_event):
    data = cloud_event.data
    read_data(data["bucket"], data["name"])
"""

read_data("parserbucket103982", "google_fde_messy_client_migration")
