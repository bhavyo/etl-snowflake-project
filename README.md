This project demonstrates an end-to-end ETL workflow using Python, Pandas and Snowflake.
It includes:
- Data cleaning using Python and Pandas
- Data loading into Snowflake
- SQL-based data transformations
- A basic ETL archtecture (raw -> cleaned -> curated layers)
- Best practices followed for Data Engineering

This project is a part of my learning path to become a Data Engineer.

## Orchestration

The pipeline is orchestrated using Snowflake Tasks with dependency chaining:
RAW → CLEAN → CURATED
Tasks are defined in a dedicated TASKS schema and executed in sequence to ensure data consistency.