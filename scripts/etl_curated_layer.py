import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

# --------------------
# Connect to Snowflake
# --------------------
#Load env variables

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

print("Connected to Snowflake securely.")

# --------------------
# Load clean data
# --------------------

df = pd.read_sql("Select * from clean_employees_python;",conn)
df.columns = [c.upper() for c in df.columns]

print("\nLoaded Clean Data:")
print("\n",df)

# --------------------
# Curated transformations
# --------------------

#Days Since Joined
today = pd.Timestamp.now().normalize()
df["DAYS_SINCE_JOIN"]= (today - pd.to_datetime(df['JOIN_DATE'], errors = 'coerce')).dt.days

#High Earner Flag
df["IS_HIGH_EARNER"]=df["SALARY"].apply(lambda x: 1 if pd.notnull(x) and x >=80000 else 0)

#Salary rank within department
df["DEPT_SALARY_RANK"] = (df.groupby("DEPARTMENT")["SALARY"].rank(method = "dense", ascending=False))

# --------------------
# Prepare for Snowflake
# --------------------

df["JOIN_DATE"]=pd.to_datetime(df["JOIN_DATE"], errors="coerce").dt.strftime("%Y-%m-%d")
df["JOIN_DATE"]=df["JOIN_DATE"].where(df["JOIN_DATE"].notna(),None)

df_write = df[["ID","NAME","DEPARTMENT","SALARY","JOIN_DATE","DAYS_SINCE_JOIN","IS_HIGH_EARNER","DEPT_SALARY_RANK"]].copy()

# --------------------
# Create curated table
# --------------------

conn.cursor().execute ("""
CREATE OR REPLACE TABLE PROJECT_DB.CURATED.EMPLOYEE_ANALYTICS_PYTHON (
    ID INT,
    NAME STRING,
    DEPARTMENT STRING,
    SALARY NUMBER,
    JOIN_DATE DATE,
    DAYS_SINCE_JOIN INT,
    IS_HIGH_EARNER INT,
    DEPT_SALARY_RANK INT
)
""")

# --------------------
# Load curated data
# --------------------

success, nchunks, nrows, _ = write_pandas(
    conn, df_write, "EMPLOYEE_ANALYTICS_PYTHON", schema = "CURATED"
)

print(f"Loaded {nrows} rows into CURATED.EMPLOYEE_ANALYTICS_PYTHON table.")
conn.close()