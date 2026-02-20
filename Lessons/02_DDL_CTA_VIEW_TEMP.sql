/*
Category: 
    Data Definition Language
    ------------------------
Purporse: 
    Defines and modifies database structure
Affects:
    Schema (Tables, views, indexes)
Example Commands: 
    CREATE. ALTER, DROP, TRUNCATE
Transaction Behaviour:
    Auto-commits by default (changes are permenant)
Usage Frequency:
    Used mainly during database setup of schema update
Example Query:
    CREATE TABLE employee(id INT, name TEXT);
*/

/*
Purpose of script:
-------------------
1. Create job_postings_flat table inside the staging schema
2. Create priority_jobs_flat_view inside the main schema
3. Create a temp table to only look at the senior jobs
*/

/*
Notes: 
------
CTAs: Create Table As Select
Views: Virtual Table, (this is a query) whenever someone goes
to this view the query executes and provides this virtual table
Temp Table: A temp table that only exists for that session

Because Views run a query, they show an updated view of the table i.e.
real time data, whereas CTAs show a snapshop for when that table was created.
So for example, if a CTA was created when a table had 10 rows of data, 
and then the table was updated to add another row, a view created of that table 
would show 11 rows whereas CTA would show only 10

Temp tables are used if there is a complex query that returns a results
and you want to use that data in a later query, think of it similar to a CTE

Syntax:
-------
CTAs: 

CREATE [OR REPLACE] TABLE table_name AS
SELECT ....

View:
CREATE [OR REPALCE] VIEW view_name AS
SELECT ....

TEMP
CREATE [OR REPLACE] TEMP TABLE table_name AS
SELECT ...
*/


-- Run script: 
-- .read Lessons/02_DDL_CTA_VIEW_TEMP.sql

-- CTAs: Create Job_postings_flat table: 
    -- Combines job_postings_fact with companies_dim

USE job_mart;

CREATE OR REPLACE TABLE staging.job_postings_flat AS
SELECT
    jpf.job_id,
    jpf.job_title_short,
    jpf.job_title,
    jpf.job_location,
    jpf.job_via,
    jpf.job_schedule_type,
    jpf.job_work_from_home,
    jpf.search_location,
    jpf.job_posted_date,
    jpf.job_no_degree_mention,
    jpf.job_health_insurance,
    jpf.job_country,
    jpf.salary_rate,
    jpf.salary_year_avg,
    jpf.salary_hour_avg,
    cd.name AS company_name
FROM data_jobs.job_postings_fact jpf
LEFT JOIN data_jobs.company_dim cd ON jpf.company_id = cd.company_id;

-- Create View of priority_jobs_flat_view
-- Combines priority_roles and job_postings_flat

CREATE OR REPLACE VIEW staging.priority_flat_view AS
SELECT
    jpf.job_id,
    jpf.job_title_short,
    jpf.job_title,
    jpf.job_location,
    jpf.job_via,
    jpf.job_schedule_type,
    jpf.job_work_from_home,
    jpf.search_location,
    jpf.job_posted_date,
    jpf.job_no_degree_mention,
    jpf.job_health_insurance,
    jpf.job_country,
    jpf.salary_rate,
    jpf.salary_year_avg,
    jpf.salary_hour_avg,
    jpf.company_name,
    pr.role_id,
    pr.role_name,
    pr.priority_level
FROM staging.job_postings_flat AS jpf
LEFT JOIN staging.priority_roles AS pr
    ON jpf.job_title_short = pr.role_name
WHERE pr.priority_level = 1;

-- Create Temp Table
/*
Create a Temp Table, that shows skills associated with Junior, Mid-Level and Senior roles. 
Junior roles will have: 
    - Junior, Jr, Graduate and Intern
Senior level roles will have: 
     - Senior,
     -Sr, 
     - Lead, 
     - Principa; 
Mid-level roles will be the ones that are left

Show: 
    - Seniority Level
    - skills associated 
    - job count
    - min salary
    - average salary
    - max salary

Hint*: You may want to filter out job postings with less than 3 posts to filter out noise
*/
CREATE OR REPLACE TEMPORARY TABLE seniority_grouped_temp AS
SELECT
    pfv.job_title_short AS job_title,
    CASE
        WHEN LOWER(pfv.job_title) LIKE '%junior%'
          OR LOWER(pfv.job_title) LIKE '%jr %'
          OR LOWER(pfv.job_title) LIKE '%graduate%'
          OR LOWER(pfv.job_title) LIKE '%intern%' THEN 'Junior'
        WHEN LOWER(pfv.job_title) LIKE '%senior%'
          OR LOWER(pfv.job_title) LIKE '%sr %'
          OR LOWER(pfv.job_title) LIKE '%lead%'
          OR LOWER(pfv.job_title) LIKE '%principal%' THEN 'Senior'
        ELSE 'Mid-Level'
    END AS seniority_level,
    skills,
    ROUND(MIN(salary_year_avg), 0) AS min_salary,
    ROUND(AVG(salary_year_avg),0) AS avg_salary,
    ROUND(MAX(salary_year_avg), 0) AS max_salary,
    COUNT(*) AS job_count
FROM staging.priority_flat_view pfv
INNER JOIN data_jobs.skills_job_dim sjd
  ON pfv.job_id = sjd.job_id
INNER JOIN data_jobs.skills_dim sd
  ON sjd.skill_id = sd.skill_id
WHERE
  pfv.priority_level = 1
AND
  pfv.job_country = 'United Kingdom'
AND
  pfv.salary_year_avg IS NOT NULL
GROUP BY seniority_level, pfv.job_title_short, skills
HAVING COUNT(*) >= 3
ORDER BY job_title, seniority_level, avg_salary DESC;


-- Now using the Temp table created above create a query that:
-- Shows the average salary per skill for junior, mid level and senior roles along with the job postings
SELECT
    skills,
    ROUND(AVG(CASE WHEN "seniority_level" = 'Junior' THEN avg_salary END), 0) AS avg_salary_junior,
    ROUND(AVG(CASE WHEN "seniority_level" = 'Mid-Level' THEN avg_salary END), 0) AS avg_salary_mid,
    ROUND(AVG(CASE WHEN "seniority_level" = 'Senior' THEN avg_salary END), 0) AS avg_salary_senior,
    SUM(job_count) AS total_job_count
FROM
    seniority_grouped_temp
GROUP BY  skills
ORDER BY total_job_count DESC;


-- DELETE -> targeted rows

-- Count of rows in tables before deleting
SELECT COUNT(*) FROM staging.job_postings_flat;
SELECT COUNT(*) FROM staging.priority_flat_view;
SELECT COUNT(*) FROM seniority_grouped_temp;

-- DELETE rows where job posted data is before 2024
DELETE FROM staging.job_postings_flat
WHERE job_posted_date < DATE '2024-01-01';

SELECT COUNT(*) FROM staging.job_postings_flat;
SELECT COUNT(*) FROM staging.priority_flat_view;
SELECT COUNT(*) FROM seniority_grouped_temp;

-- Notice how the temp table was not changed, because it is a snapshot of when it was created
-- and thus not updated

-- Truncate -> Fast Wipe, removes all rows but keeps collumns
