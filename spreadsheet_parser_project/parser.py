#These are my goals with this project:
#
#Parses multiple files without any error
#Creates and validates schema and omits empty columns
#Hashes PII
#Accessible by AI (which has safety checks and serve a specific role) using the RAG architecture
import pandas as pd
import hashlib
import re
from math import isnan
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, field_validator, ConfigDict, ValidationError
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage
#import functions_framework

api = FastAPI()
bq = bigquery.Client()
pantry = storage.Client()
table = "rugged-coil-514202-s2.pydantic_parsing_test"
quarantine = []

schema_field = [bigquery.SchemaField("Transaction_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("Customer_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Units", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Unit_Price_USD", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("Total_Amount_USD", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("Department", "STRING", mode="NULLABLE"), 
    bigquery.SchemaField("Region", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Product_SKU", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("Status", "STRING", mode="NULLABLE")
    ]

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema_field)

#Make it so that 

key_words = {
        "Date" : r"date|time|created",
        "Transaction_ID" : r"order|txn|transaction",
        "Customer_ID" : r"account|acct|customer|client",
        "Status" : r"status",
        "Product_SKU" : r"sku|product",
        "Unit_Price_USD" : r"price|cost",
        "Total_Amount_USD" : r"total|amount",
        "Region" : r"region",
        "Units" : r"units|quantity|qty",
        "Department" : r"department"
    }




class SchemaValidator(BaseModel):  
    Date : datetime
    Transaction_ID : str
    Customer_ID : str
    Status : str | None = None
    Product_SKU : str | None = None
    Unit_Price_USD : float
    Total_Amount_USD : float
    Region: str | None = None
    Units: int
    Department : str | None = None
    model_config = ConfigDict(extra='ignore')

    @field_validator('Date', mode='before')
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
                return value  # Let Pydantic catch invalid dates and send to quarantine

    @field_validator('*', mode='before')
    @classmethod
    def convert_to_none(cls, value):
            if isinstance(value, float) and isnan(value):
                return None
            if str(value) and str(value).strip().lower() in ("nan", "nat"):
                return None
            return value

def schema_matching(data):
        
        for schemName, value in key_words.items():
            col = data.columns[data.columns.str.contains(value, case=False, na=False)] #matches colums using false/true as the index
            
            if not col.empty:
                data = data.rename(columns={col[0]: schemName})

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
                data["Total_Amount_USD"] = (data["Total_Amount_USD"].astype(str).str.replace(r"[^0-9.]", "", regex=True))
                data["Total_Amount_USD"] = pd.to_numeric(data["Total_Amount_USD"], errors="coerce") #Converts the string back into a number.
                data["Units"] = (data["Units"].astype(str).str.replace(r"[^0-9.]", "", regex=True))
                data["Units"] = pd.to_numeric(data["Units"], errors="coerce")
                data["Unit_Price_USD"] = data["Total_Amount_USD"] / data["Units"]
                        
        return data


def schema_validation(file):
    file = file.where(pd.notnull(file), None) #can use pd.notnull() even though it's an alias of pd.notna()
    records = file.to_dict(orient="records")
    valid_row = []

    for row_index, row in enumerate(records, start=1): #Using itertuples after i get this working
        try: 
            validated_data = SchemaValidator.model_validate(row)
            valid_row.append(validated_data)
        except ValidationError as error:
            print(error)
            print(f"Schema validation failed, moving line at {row_index} into quarantine")
            quarantine.append(row)
    if not valid_row:
        return pd.DataFrame()
    return pd.DataFrame([row.model_dump() for row in valid_row])

def sanitize_columns(file):
    file.columns = [str(col).strip() for col in file.columns]
    file.columns = [re.sub(r'[^a-zA-Z0-9_]', '', col) for col in file.columns]
    return file

def find_data_excel(file):
    with file.open('rb') as excel_bytes: #Reads it as bytes so it can be put into pd.ExcelFile() for traversing its sheets.
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
                c = pd.read_csv(file.open("rb"), sep=sep, skiprows=skip, encoding="utf-8") #Delimiter is found, so read it as bytes for proper parsing.
                return schema_validation(schema_matching(sanitize_columns(c)))
                

def upload(file_name, file):
        table_name = table+"."+file_name
        bq.load_table_from_dataframe(file, table_name, job_config=job_config).result()

def read_data(bucket_name, blob_name):
    bucket = pantry.bucket(bucket_name) #Retrieves and iterates the contents in the bucket.
    blobby = bucket.list_blobs(prefix=blob_name)
    for file in blobby:
        if (not file.name.endswith("/") and not file.name.endswith(".DS_Store") and not file.name.endswith("audit_ground_truth.csv")):
            if "accounts_master" in file.name:
                job_config2 = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"
                )
                print(table+"."+Path(file.name).stem)
                a_m = pd.read_csv(file.open('rb'))
                bq.load_table_from_dataframe(a_m, table+"."+Path(file.name).stem, job_config=job_config2).result()
            try:
                file_name = Path(file.name).stem
                if (file.name.endswith(".xlsx") or file.name.endswith(".xls")):
                    file = find_data_excel(file)
                elif (file.name.endswith(".csv")):
                    file = find_delimiter(file)
                upload(file_name, file)
            except Exception as e:
                print(f"Error processing file {file.name}: {e}")


stringgg = """@functions_framework.cloud_event
def new_file(cloud_event):
    data = cloud_event.data
    read_data(data["bucket"], data["name"])
"""

read_data("parserbucket103982", "google_fde_messy_client_migration")
print(quarantine)