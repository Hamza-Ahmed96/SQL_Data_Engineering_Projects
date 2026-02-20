
/*
Category: 
    Data Manipulation Language
    ------------------------
Purpose: 
    Manages and manipulates data within tables
Affects:
    Actual records
Example Commands: 
    SELECT, INSERT, UPDATE, DELETE
Transaction Behaviour:
    Can be rolled back (part of transaction)
Usage Frequency:
    Used daily for working with data
Example Query:
    INSERT INTO employees VALUES
    (1, 'Luke');

*/


-- .read Lessons/01_DML_CREATE_ALTER_INSERT_UPDATE.sql
-- Detach from job_mart database
USE sample_data;

DROP DATABASE IF EXISTS job_mart;

-- Create job_mart database (if it doesn't already exist)
CREATE DATABASE IF NOT EXISTS job_mart;

-- Create the staging schema inside job_mart
USE job_mart;
CREATE SCHEMA IF NOT EXISTS staging;

/*
-- Check that staging schema was created 
SELECT
*
FROM information_schema.schemata;
*/

-- CREATE priority_roles table

DROP TABLE IF EXISTS staging.preferred_roles;
DROP TABLE IF EXISTS staging.priority_roles;

CREATE TABLE IF NOT EXISTS staging.preferred_roles(
    role_id INTEGER PRIMARY KEY,
    role_name VARCHAR
);

-- Check that the table preferred roles exists in the staging area:
/* 
SELECT
*
FROM information_schema.tables
WHERE table_catalog = 'job_mart'; 
*/

-- INSERT

-- Transaction 1: Insert rows and set boolean values
-- If any statement fails, all changes roll back
BEGIN TRANSACTION;

INSERT INTO staging.preferred_roles(role_id, role_name)
VALUES
    (1, 'Data Engineer'),
    (2, 'Senior Data Engineer'),
    (3, 'Data Scientist');

-- Alter Table
ALTER TABLE staging.preferred_roles
ADD COLUMN preferred_role BOOLEAN;

-- Update to add data to the preferred role column
UPDATE staging.preferred_roles
SET preferred_role = TRUE
WHERE role_id = 1 OR role_id = 3;

UPDATE staging.preferred_roles
SET preferred_role = FALSE
WHERE role_id = 2;

COMMIT;

SELECT
*
FROM staging.preferred_roles;

-- Change column name and table name

-- Change table name to priority roles
ALTER TABLE staging.preferred_roles
RENAME TO priority_roles;

-- Change Column name to priority_role
ALTER TABLE staging.priority_roles
RENAME COLUMN preferred_role TO priority_level;

-- Change Column Values
ALTER TABLE staging.priority_roles
ALTER COLUMN priority_level TYPE INTEGER;

-- Transaction 2: Update role names and priority levels
-- All updates succeed together or none apply
BEGIN TRANSACTION;

-- Update Senior Data Engineer to Data Analyst
UPDATE staging.priority_roles
SET role_name = 'Data Analyst'
WHERE role_name = 'Senior Data Engineer';
-- Set priority level to 1 for data Analyst
UPDATE staging.priority_roles
SET priority_level = 1
WHERE role_name = 'Data Analyst';
-- Set priority level to 2 for data engineer
UPDATE staging.priority_roles
SET priority_level = 2
WHERE role_name = 'Data Engineer';
-- Set priority level to 3 for Data Scientist
UPDATE staging.priority_roles
SET priority_level = 1
WHERE role_name = 'Data Scientist';

COMMIT;

SELECT
*
FROM staging.priority_roles;