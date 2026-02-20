-- Create TEMP TABLE as source table
CREATE OR REPLACE TEMPORARY TABLE src_priority_jobs AS
SELECT
    jpf.job_id,
    jpf.job_title_short,
    pr.role_name,
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


-- MERGE INTO
MERGE INTO main.priority_jobs_snapshot AS tgt
USING src_priority_jobs AS src
ON tgt.job_id = src.job_id

-- Update : mathced rows where values differ
WHEN MATCHED AND tgt.priority_level IS DISTINCT FROM src.priority_level THEN
    UPDATE SET
        priority_level = src.priority_level,
        updated_at = src.updated_at

-- INSERT : matched rows where you want to remove
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        job_id,
        job_title_short,
        company_name,
        job_posted_date,
        salary_year_avg,
        priority_level,
        updated_at 
    )
    VALUES (
        src.job_id,
        src.job_title_short,
        src.company_name,
        src.job_posted_date,
        src.salary_year_avg,
        src.priority_level,
        src.updated_at
    )

-- DELETE
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;


-- Check AFTER 
SELECT
    job_title_short,
    COUNT(*) AS job_count,
    MIN(priority_level) AS priority_level,
    MIN(updated_at) AS updated_at
FROM priority_jobs_snapshot
GROUP BY job_title_short
ORDER BY job_count DESC;
