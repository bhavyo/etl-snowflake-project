import os
import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

#load env vars
load_dotenv()

print("ACCOUNT:", os.getenv("SNOWFLAKE_ACCOUNT"))


# -------------------
# Call API
# -------------------

url = "https://fakestoreapi.com/users"
response = requests.get(url)
response.raise_for_status

data=response.json()

print(f"Fetched {len(data)} records from API")

# -------------------
# Normalize JSON
# -------------------

rows = []

for user in data:
    rows.append({
        "ID":user["id"],
        "EMAIL":user["email"],
        "USERNAME":user["username"],
        "FIRSTNAME":user["name"]["firstname"],
        "LASTNAME":user["name"]["lastname"],
        "CITY":user["address"]["city"],
        "PHONE":user["phone"]
    })

df = pd.DataFrame(rows)
print("\nRaw API Data:")
print(df.head())

# -------------------
# Connect to Snowflake
# -------------------

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="RAW"
)

print("Connected to Snowflake")

#Ensure RAW schema context
conn.cursor().execute("USE SCHEMA PROJECT_DB.RAW")

# -------------------
# Load into RAW table
# -------------------

sucess,nchunks,nrows,_=write_pandas(
    conn,
    df,
    "RAW_API_USERS",
    schema = "RAW"
)

print(f"Loaded {nrows} rows into RAW_API_USERS")

conn.close()

#Doubts
#response.raise_for_status
#print(df.head())
#sucess,nchunks,nrows,_=write_pandas(