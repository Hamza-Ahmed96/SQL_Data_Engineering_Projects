-- Create a priority roles table that includes role names and thier associated prioroty level. 
USE job_mart;
BEGIN TRANSACTION;
    CREATE OR REPLACE TABLE staging.priority_roles (
        role_id INTEGER PRIMARY KEY,
        role_name VARCHAR,
        priority_level INTEGER
    );
    INSERT INTO staging.priority_roles(role_id, role_name, priority_level)
    VALUES
    (1, 'Data Engineer', 3),
    (2, 'Data Scientist', 1),
    (3, 'Data Analyst', 1),
    (4, 'Senior Data Scientist', 2),
    (5, 'Senior Data Analyst', 1),
    (6, 'Senior Data Engineer', 4);
COMMIT;


-- Show the table was created. 
SELECT
*
FROM staging.priority_roles;
