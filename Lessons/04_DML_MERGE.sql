/*

Processing:
------------

Batch Processing:
------
- Scheduled : Runs on fixed intervals
- Chuncked : Processes data in groups
- Incremental-friendly : Only new/ changed rows
- Warehouse-native: Default for analytical stakcs 

- Tools : dbt, Airflow, DuckDB, Snowflake

Continious Processing: 
- Real time : Processes data as it arrives 
- Event Driven : Each record triggers logic
- Low latency 
- Ops-naitve drive : for live systems 

Tools: Kfka, Flink, Spark Streaming
*/

/*
Here we will be building priority_jobs_snapshot table inside 
job_mart main
This table combines job_postings_fact with company_dim and priority_roles
*/

/*
1. A Script called priority_roles.sql, which is a relatavily small script 
creates a table called priority roles. It is production ready. 
*/

-- The script is duplicated here for ease of reading
-- Part of DML part 3 
USE job_mart;
BEGIN TRANSACTION;
    CREATE OR REPLACE TABLE staging.priority_roles (
        role_id INTEGER PRIMARY KEY,
        role_name VARCHAR,
        priority_level INTEGER
    );
    INSERT INTO staging.priority_roles(role_id, role_name, priority_level)
    VALUES
    (1, 'Data Engineer', 2),
    (2, 'Data Scientist', 1),
    (3, 'Data Analyst', 1),
    (4, 'Software Engineer', 3);
COMMIT;


-- Show the table was created. 
SELECT
*
FROM staging.priority_roles;
