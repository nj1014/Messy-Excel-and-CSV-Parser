#These are my goals with this project:
#
#Parses multiple files without any error
#Creates and validates schema and omits empty columns
#Hashes PII
#Accessible by AI (which has safety checks and serve a specific role) using the RAG architecture

#RIGHT NOW ACCORDING TO GCP AGENT ABOUT 68.33% of records are fully compliant without requiring manual cleaning or fallback imputation FOR ONE FILE
from email import header
import io
import pandas as pd
import hashlib
import re
import csv
from datetime import date, datetime, timezone
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, field_validator, TypeAdapter, ValidationError, model_validator
from functools import partial
from collections import defaultdict
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage
import functions_framework

api = FastAPI()
parsed_data = "/parsed_Data/"
bq = bigquery.Client()
pantry = storage.Client()
messy_data = "/spreadsheets/"
table = "rugged-coil-514202-s2.pydantic_parsing_test"
quarantine = []

job_config = bigquery.LoadJobConfig(schema_update_options=[
    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION]
    )

key_words = {
        "Date" : "date|time|created",
        "Transaction_ID" : "order|txn|transaction",
        "Customer_ID" : "account|acct|customer",
        "Status" : "status",
        "Product_SKU" : "sku|product",
        "Unit_Price_USD" : "price|cost",
        "Total_Amount_USD" : "total|amount",
        "Region" : "region",
        "Units" : "units|quantity|qty",
        "Department" : "department"
    }

class SchemaValidator(BaseModel):  
    Date : str
    Transaction_ID : str
    Customer_ID : str
    Status : str | None = None
    Product_SKU : str | None = None
    Unit_Price_USD : float | None = None #Should be required, but I'm testing my Pydantic model.
    Total_Amount_USD : float #Should be required, but I'm testing my Pydantic model.
    Region: str | None = None
    Units: int
    Department : str | None = None




def schema_matching(data):
        for schemName, value in key_words.items():
            col = data.columns[data.columns.str.contains(value, case=False, na=False)] #matches colums using false/true as the index
            
            if not col.empty:
                data = data.rename(columns={col[0]: schemName})

        if data.shape[1] < 10:
            missing_columns = key_words.keys() - set(data.columns)
            for column in missing_columns:
                data[column] = None
                if "Total_Amount_USD" in column and not "Unit_Price_USD" in missing_columns:
                    data["Unit_Price_USD"] = (data["Unit_Price_USD"].astype(str).str.replace(r"[^0-9.]", "", regex=True))
                    data["Unit_Price_USD"] = pd.to_numeric(data["Unit_Price_USD"], errors="coerce") #Converts the string back into a number.
                    data["Units"] = (data["Units"].astype(str).str.replace(r"[^0-9.]", "", regex=True))
                    data["Units"] = pd.to_numeric(data["Units"], errors="coerce")
                    data["Total_Amount_USD"] = data["Unit_Price_USD"] * data["Units"]
                elif "Unit_Price_USD" in column and not "Total_Amount_USD" in missing_columns:
                    data["Unit_Price_USD"] = data["Total_Amount_USD"] / data["Units"]
                        
        return data


def schema_validation(file):
    file = file.where(pd.notnull(file), None)
    file = file.to_dict(orient="records")
    valid_row = []

    for row_index, row in enumerate(file, start=1): #Using itertuples after i get this working
        try: 
            validated_data = SchemaValidator.model_validate(row)
            valid_row.append(validated_data)
        except ValidationError as error:
            print(f"Schema validation failed, moving line at {row_index} into quarantine")
            quarantine.append(row)
    if not valid_row:
        return pd.DataFrame()
    return pd.DataFrame([row.model_dump() for row in valid_row])

def sanitize_columns(file):
    # This function will sanitize the lines in the file to ensure they are valid JSON.
    file.columns = [str(col).lower() for col in file.columns]
    file.columns = [col.strip() for col in file.columns]
    file.columns = [re.sub(r'[^a-zA-Z0-9_]', '_', str(col)).strip() for col in file.columns]
    file.columns = [re.sub(r'_+', '_', col).strip('_') for col in file.columns]
    file.columns = [("_" + col) if col and col[0].isdigit() else col for col in file.columns]
    return file

def find_data_excel(file):
    with io.BytesIO(file.download_as_bytes()) as excel_bytes: #Safe as each excel file is about 8 KB. This line of code goes throguh each line individually.
        xcel_file = pd.ExcelFile(excel_bytes)
        sheett = 0
        for sheet in xcel_file.sheet_names:
            if "summary" in sheet.lower() or "cover" in sheet.lower():
                sheett+=1
                continue
            x = pd.read_excel(xcel_file, sheet_name=sheett, header=None)
            if x.shape[0] > 3 and x.shape[1] > 2:
                index = x[0].first_valid_index()
                c = pd.read_excel(xcel_file, sheet_name=sheett, skiprows=index)
                return schema_validation(schema_matching(sanitize_columns(c)))
    return pd.DataFrame()


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
                c = pd.read_csv(file.open("r"), sep=sep, skiprows=skip, encoding="utf-8")
                return schema_validation(schema_matching(sanitize_columns(c)))
                

def upload(folder):
    for item_parent, item in folder.items():
        for folder_name, file in item:
            folder_name = folder_name.replace("/", "-")
            table_name = table+"."+folder_name
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
                if (file.name.endswith(".xlsx") or file.name.endswith(".xls")):
                    folder[str(Path(file.name).parent)].append((Path(file.name).stem, find_data_excel(file)))
                elif (file.name.endswith(".csv")):
                    folder[str(Path(file.name).parent)].append((Path(file.name).stem, find_delimiter(file)))
                upload(folder)
            except Exception as e:
                print(f"Error processing file {file.name}: {e}")


stringgg = """@functions_framework.cloud_event
def new_file(cloud_event):
    data = cloud_event.data
    read_data(data["bucket"], data["name"])
"""

datetime_function = """    @field_validator('Date', mode='before')
    @classmethod
    def convert_to_timestamp(cls, value):
            # 1. If already a timestamp, pass it through
            if value is None:
                return value
            try:
                if isinstance(value, datetime):
                    return pd.to_datetime(value, utc=True)
                if isinstance(value, (int, float)):
                    return pd.to_datetime(value, utc=True, origin="unix", unit="s")
                return pd.to_datetime(value, utc=True, format="mixed")
            except Exception:
                return value  # Let Pydantic catch invalid dates and send to quarantine"""


read_data("parserbucket103982", "google_fde_messy_client_migration")
