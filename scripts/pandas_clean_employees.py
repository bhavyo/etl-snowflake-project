import pandas as pd

# Sample raw data (simulating messy data as it comes from CSV or external source)
data = {
    "id": [1, 2, 3, 4, 5],
    "name": [" Amit ", "Priya", "RAHUL", "Sneha", ""],
    "department": ["Engineering", "Marketing", "HR", "Engineering", "Finance"],
    "salary": ["90000", "65000", "N/A", "null", "70000"],
    "join_date": ["2022-01-10", "2021-05-02", "2020-13-40", "2023-02-14", "2021-07-30"]
    
}

# Create DataFrame
df = pd.DataFrame(data)
print("Raw DataFrame:\n", df , "\n")

# Clean name: Strip spaces, standardise invalid values to None
df["name"] = df["name"].str.strip()
df["name"] = df["name"].replace({"N/A":None, "n/a":None, "null":None, "":None})

# Normalise department
df["department"] = df["department"].str.title()

# Convert salary from string to number
df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

# Convert join_date to datetime and handle invalid dates
df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")


print("Cleaned Dataframe:\n", df , "\n")

# Save cleaned output:

df.to_csv("data/clean_employees_panda.csv", index = False)

print ("Saved cleaned data to clean_employees_panda.csv")



