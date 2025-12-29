import pandas as pd
import snowflake.connector

# ------------------------------
# 1. Snowflake Connection Details
# ------------------------------

conn = snowflake.connector.connect(
    user="bhavyo",
    password="KimNamjoon@1994",
    account="zv11927.ap-southeast-1",
    warehouse="COMPUTE_WH",
    database="PROJECT_DB",
    schema="RAW"

)

print("Connected to Snowflake successfully!")

# ------------------------------
# 2. Read from Snowflake
# ------------------------------

query = "SELECT * FROM RAW_EMPLOYEES;"
df_raw = pd.read_sql(query,conn)

print("\nRaw Data from Snowflake:")
print(df_raw)

# ------------------------------
# 3. Clean using pandas
# ------------------------------

df_clean = df_raw.copy()

# Normalize column names (Snowflake returns uppercase usually)
df_clean.columns = [c.upper() for c in df_clean.columns]

#Clean name column
df_clean["NAME"] = df_clean["NAME"].astype(str).str.strip()
df_clean["NAME"] = df_clean["NAME"].replace({"N/A":None, "null":None,"":None})

#Convert Salary to numeric
df_clean["SALARY"] = pd.to_numeric(df_clean["SALARY"], errors="coerce")
df_clean["SALARY"] = df_clean["SALARY"].where(df_clean["SALARY"].notna(), None)

#Convert Join_Date to DateTime
df_clean["JOIN_DATE"] = pd.to_datetime(df_clean["JOIN_DATE"], errors = "coerce")
df_clean["JOIN_DATE"] = df_clean["JOIN_DATE"].dt.strftime('%Y-%m-%d')

print("\nCleaned Data:")
print(df_clean)


# ------------------------------
# 4. Write cleaned data back to Snowflake
# ------------------------------

#Create table id not exists
create_table_sql= """
CREATE OR REPLACE TABLE CLEAN.CLEAN_EMPLOYEES_PYTHON (
    ID INT,
    NAME STRING,
    DEPARTMENT STRING,
    SALARY NUMBER,
    JOIN_DATE DATE
);
"""
conn.cursor().execute(create_table_sql)

# ------------------------------
# Write cleaned data to Snowflake
# ------------------------------
# Ensure df columns are uppercase and in correct order
desired_cols = ["ID", "NAME", "DEPARTMENT", "SALARY", "JOIN_DATE"]
df_write = df_clean[desired_cols].copy()

from snowflake.connector.pandas_tools import write_pandas

success, nchunks, nrows, _ = write_pandas(conn, df_write, "CLEAN_EMPLOYEES_PYTHON", schema="CLEAN")
print(f"\nwrite_pandas result: success={success}, nchunks={nchunks}, nrows={nrows}")


# ------------------------------
# 5. Close connection
# ------------------------------

conn.close()
print("\nConnetion closed.")