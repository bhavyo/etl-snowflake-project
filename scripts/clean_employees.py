import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

#load environment variables
load_dotenv()

#Connect to Snowflake
conn = snowflake.connector.connect(
    user = os.getenv("SNOWFLAKE_USER"),
    password = os.getenv("SNOWFLAKE_PASSWORD"),
    account =os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse =os.getenv("SNOWFLAKE_WAREHOUSE"),
    database = os.getenv("SNOWFLAKE_DATABAESE"),
    schema = "RAW"
)

#Read from RAW table
query = "SELECT * FROM PROJECT_DB.RAW.RAW_EMPLOYEES_STAGE;"
df = pd.read_sql(query,conn)

print("\nRaw Data:")
print(df)

# ------------------
# CLEANING LOGIC
# ------------------

#Normalise empty strings
df["NAME"] = df["NAME"].str.strip()
df["NAME"] = df["NAME"].replace({"N/A":None, "n/a":None, "null":None, "":None})


#Standardize Department Names
df["DEPARTMENT"]=df["DEPARTMENT"].str.title()

#Salary to numeric
df["SALARY"]=pd.to_numeric(df["SALARY"], errors="coerce")

#Date parsig
df["JOIN_DATE"]=pd.to_datetime(df["JOIN_DATE"],errors="coerce").dt.date

#Drop duplicate IDs
df = df.drop_duplicates(subset=["ID"])

print("\nCleaned Data:")
print(df)

#Switch to clean schema
conn.cursor().execute("USE SCHEMA PROJECT_DB.CLEAN;")

#Load to Clean Layer
conn.cursor().execute("DELETE FROM PROJECT_DB.CLEAN.CLEAN_EMPLOYEES;")
success, nchunks, nrows, _ = write_pandas(conn,df,"CLEAN_EMPLOYEES",schema = "CLEAN")

print(f"Loaded {nrows} rows in CLEAN.CLEAN_EMPLOYEES.")

conn.close()


