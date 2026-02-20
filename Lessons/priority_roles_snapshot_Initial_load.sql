-- Creates a table that combines job_postings_fact table with company_dim and priority_roles 
-- to produce a table that only shows job postings with the prioirty roles



-- Initial Load : 

/*
This should only be run once, as it creates the priority snapshot table 
with the data we have so far
Next we will be working on two batch processing loads that are meant to be run 
everyday to update this table
1. Version 1 will use Update, and Delete
2. Version 2 will use Merge

*/

USE job_mart;

BEGIN TRANSACTION;
    CREATE OR REPLACE TABLE main.priority_jobs_snapshot(
        job_id INTEGER PRIMARY KEY,
        job_title_short VARCHAR,
        role_name VARCHAR,
        company_name VARCHAR,
        job_posted_date TIMESTAMP,
        salary_year_avg DOUBLE,
        priority_level INTEGER,
        updated_at TIMESTAMP
    );

    INSERT INTO main.priority_jobs_snapshot (
        job_id,
        job_title_short,
        role_name,
        company_name,
        job_posted_date,
        salary_year_avg,
        priority_level,
        updated_at
    )
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
COMMIT;


-- Check if the table was correctly created.
SELECT
    role_name,
    COUNT(*) AS job_count,
    MIN(priority_level) AS priority_level,
    MIN(updated_at) AS updated_at
FROM priority_jobs_snapshot
GROUP BY role_name
ORDER BY job_count DESC;

-- After Initial Load : 

/*
─────────────────┬───────────┬────────────────┬───────────────────────────────|
│  job_title_short  │ job_count │ priority_level │         updated_at         │
│      varchar      │   int64   │     int32      │         timestamp          │
├───────────────────┼───────────┼────────────────┼────────────────────────────┤
│ Data Analyst      │    408640 │              1 │ 2026-02-16 12:57:56.511487 │
│ Data Engineer     │    391957 │              2 │ 2026-02-16 12:57:56.511487 │
│ Data Scientist    │    331002 │              1 │ 2026-02-16 12:57:56.511487 │
│ Software Engineer │     92271 │              3 │ 2026-02-16 12:57:56.511487 │
└───────────────────┴───────────┴────────────────┴────────────────────────────┘
*/