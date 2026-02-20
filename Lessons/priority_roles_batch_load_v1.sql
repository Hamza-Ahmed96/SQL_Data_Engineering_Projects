/*
Task:
-----
Pipeline : Update the priority_jobs_snapshot table on a daily basis
-- Folow Notes on : Lessons/notes.md - Creating Priority_jobs_snapshot pipeline expalined: ETL pipeline to create a datawarehouse
Methodology:
------------
Source Table : Incoming data streams of job postings (Temp Table with updated data from job_postings_flat, company_dim and priority_roles)
Target Table : job_mart.main.job_postings_snapshot (table that needs updating)
*/


-- Create TEMP TABLE as source table
CREATE OR REPLACE TEMPORARY TABLE src_priority_jobs AS
SELECT
    jpf.job_id,
    jpf.job_title_short,
    cd.name AS company_name,
    jpf.job_posted_date,
    jpf.salary_year_avg,
    pr.priority_level,
    CURRENT_TIMESTAMP AS updated_at
FROM 
    data_jobs.job_postings_fact jpf 
LEFT JOIN 
    data_jobs.company_dim cd
ON 
    jpf.company_id = cd.company_id
INNER JOIN
    staging.priority_roles pr
ON 
    jpf.job_title_short = pr.role_name;

-- Check BEFORE 
SELECT
    job_title_short,
    COUNT(*) AS job_count,
    MIN(priority_level) AS priority_level,
    MIN(updated_at) AS updated_at
FROM priority_jobs_snapshot
GROUP BY job_title_short
ORDER BY job_count DESC;

/*
| Operation | What it catches | Example |
|-----------|-----------------|---------|
| **Update** | Same job_id exists in both, but values differ | Priority level changed from 3 -> 1 |
| **INSERT** | job_id in source but NOT in target | The 50 new postings |
| **DELETE** | job_id in target but NOT in source | The 10 removed postings |
*/

BEGIN TRANSACTION;
    -- UPDATE STATEMENT
    -- Update priority jobs snapshot in job_mart.main incase the priority_roles table is updated
    UPDATE main.priority_jobs_snapshot
    SET
        -- set the priority level of the target table to the source table priority level
        priority_level = src.priority_level,
        -- Update the updated at field as well
        updated_at = src.updated_at
    FROM
        -- we are updating the priority_jobs_snapshot (target table) from the source table (temp table above)
        src_priority_jobs AS src
    WHERE
        -- we are updating where the target_job id matches the 
        main.priority_jobs_snapshot.job_id = src.job_id
    AND
        -- AND target_priority level is different from the source priority level
        main.priority_jobs_snapshot.priority_level IS DISTINCT FROM src.priority_level;
COMMIT;

BEGIN TRANSACTION;
    -- Insert rows from src_priority_jobs (source) that don't already exist in main.priority_jobs_snapshot (target), matched on job_id.       
    INSERT INTO main.priority_jobs_snapshot (
        job_id,
        job_title_short,
        company_name,
        job_posted_date,
        salary_year_avg,
        priority_level,
        updated_at 
    )
    SELECT
        job_id,
        job_title_short,
        company_name,
        job_posted_date,
        salary_year_avg,
        priority_level,
        updated_at
    FROM src_priority_jobs src
    WHERE NOT EXISTS (
        SELECT 1
        FROM main.priority_jobs_snapshot tgt
        WHERE tgt.job_id = src.job_id
    );
COMMIT;

-- Delete rows from target table (main.job_postings_snapshot) that don't exist in the source table src_priority_jobs, matched on job_id
DELETE FROM main.priority_jobs_snapshot AS tgt
WHERE NOT EXISTS (
    SELECT 1
    FROM src_priority_jobs src
    WHERE src.job_id = tgt.job_id
);

-- Check AFTER 
SELECT
    job_title_short,
    COUNT(*) AS job_count,
    MIN(priority_level) AS priority_level,
    MIN(updated_at) AS updated_at
FROM priority_jobs_snapshot
GROUP BY job_title_short
ORDER BY job_count DESC;
