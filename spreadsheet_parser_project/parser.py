#These are my goals with this project:
#
#Parses multiple files without any error
#Creates and validates schema and omits empty columns
#Hashes PII
#Accessible by AI (which has safety checks and serve a specific role) using the RAG architecture

#RIGHT NOW ACCORDING TO GCP AGENT ABOUT 68.33% of records are fully compliant without requiring manual cleaning or fallback imputation FOR ONE FILE
from email import header

import pandas as pd
import hashlib
import re
import csv
from functools import partial
from collections import defaultdict
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage
import functions_framework

parsed_data = "/parsed_Data/"
bq = bigquery.Client()
pantry = storage.Client()
messy_data = "/spreadsheets/"
table = "rugged-coil-514202-s2.messy_data_parsing_project"

job_config = bigquery.LoadJobConfig(schema_update_options=[
    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION]
    )

def sanitize_lines(file):
    # This function will sanitize the lines in the file to ensure they are valid JSON.
    file.columns = [col.strip() for col in file.columns]
    file.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', str(col)).strip() for col in file.columns]
    file.columns = [re.sub(r'_+', '_', col).strip('_') for col in file.columns]
    file.columns = [("_" + col) if col and col[0].isdigit() else col for col in file.columns]
    return file

def find_delimiter(file):
    with file.open('r', encoding="utf-8") as f:
        skip = 0
        for line in f:
            line = line.strip()
            if line.startswith("#"):
                skip+=1
                continue  # Skip comment lines
            else:
                sep = max((",", ";", ":", "|"), key=line.count)
                if line.count(sep) == 0:
                    raise ValueError(f"None of the separators are found in the line: {line}")
                return pd.read_csv(file.open("r"), sep=sep, skiprows=skip, encoding="utf-8")
                

def upload(folder):
    for item_parent, item in folder.items():
        for folder_name, file in item:
            folder_name = folder_name.replace("/", "-")
            table_name = table+"."+folder_name
            file = sanitize_lines(file)
            for col in file.select_dtypes(include=["object"], exclude=["str"]).columns:
                # Check the actual types inside the object column (ignoring empty NaN cells)
                inferred = pd.api.types.infer_dtype(file[col], skipna=True) #
                
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
            try:
                with file.open('r') as f:
                    if (file.name.endswith(".xlsx") or file.name.endswith(".xls")):
                        folder[str(Path(file.name).parent)].append((Path(file.name).stem, pd.read_excel(file.open("rb"))))
                    elif (file.name.endswith(".csv")):
                        folder[str(Path(file.name).parent)].append((Path(file.name).stem, find_delimiter(file))) #used partial to locate error file
                upload(folder)
            except Exception as e:
                print(f"Error processing file {file.name}: {e}")


stringgg = """@functions_framework.cloud_event
def new_file(cloud_event):
    data = cloud_event.data
    read_data(data["bucket"], data["name"])
"""

read_data("parserbucket103982", "google_fde_messy_client_migration")
