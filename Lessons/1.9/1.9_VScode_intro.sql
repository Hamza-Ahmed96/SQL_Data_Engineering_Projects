-- Casting and Concacting 
SELECT
    job_id::VARCHAR || '-' || company_id::VARCHAR AS unique_id,
    job_work_from_home::INT AS job_work_from_home,
    job_posted_date::DATE AS job_posted_date,
    salary_year_avg::DECIMAL(10,0) AS salary_year_avg
FROM 
    job_postings_fact
WHERE salary_year_avg IS NOT NULL
LIMIT 10;

-- Create job_mart database
CREATE DATABASE IF NOT EXISTS job_mart;

-- Create the staging shcema inside job_mart
USE job_mart;
CREATE SCHEMA IF NOT EXISTS staging;

-- Check that staging schema was created 
SELECT
*
FROM information_schema.schemata;

-- CREATE priority_roles table

CREATE TABLE IF NOT EXISTS staging.preferred_roles(
    role_id INTEGER PRIMARY KEY,
    role_name VARCHAR,
);

SELECT
*
FROM information_schema.tables
WHERE table_catalog = 'job_mart';

-- INSERT 

USE job_mart;

INSERT INTO staging.preferred_roles(role_id, role_name)
VALUES
    (1, 'Data Engineer'),
    (2, 'Senior Data Engineer');

SELECT
*
FROM staging.preferred_roles;
