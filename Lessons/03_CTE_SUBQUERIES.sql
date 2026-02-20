/*
Subqueires:
----------
Nested queries:

SELECT *
FROM 
(
SELECT
*
FROM job_postings
WHERE job_title_short = 'Data Engineer'
) AS data_engineering_jobs

Common Table Expression
-----------------------
- A temporary results set that create a intermediate results tables

WITH data_engineering JOBS AS
(
SELECT
*
FROM job_postings_flat 
WHERE job_title_short = 'Data Engineer'
)
SELECT
*
FROM data_engineering_jobs
*/

-- Subqueries --

-- Question 1: Show each job's salary next to the overall market median --

-- group by job_title
-- calculate market median, 


-- Sub Query in SELECT
SELECT
    job_title_short,
    (SELECT MEDIAN(salary_year_avg) FROM job_postings_fact) AS median_market_salary,
    salary_year_avg
FROM 
    job_postings_fact
WHERE 
    salary_year_avg IS NOT NULL;


-- Subquery in FROM 
-- Stage only jobs that are remote before aggregating
SELECT
    job_title_short,
    MEDIAN(salary_year_avg) AS median_salary,
    (SELECT MEDIAN(salary_year_avg) FROM job_postings_fact WHERE job_work_from_home = True) AS remote_median_market_salary,
FROM 
    (
        SELECT
            job_title_short,
            salary_year_avg
        FROM 
            job_postings_fact
        WHERE
            job_work_from_home = True

    ) AS remote_jobs
GROUP BY job_title_short
ORDER BY median_salary;

-- Subquery in Having
-- Keep only job titles whose median salary is above the overall median
SELECT
    job_title_short,
    MEDIAN(salary_year_avg) AS median_salary,
    (SELECT MEDIAN(salary_year_avg) FROM job_postings_fact WHERE job_work_from_home = True) AS remote_median_market_salary,
FROM 
    (SELECT
            job_title_short,
            salary_year_avg
        FROM 
            job_postings_fact
        WHERE
            job_work_from_home = True) AS remote_jobs
GROUP BY job_title_short
HAVING 
median_salary > 
(SELECT
    MEDIAN(salary_year_avg) 
    FROM 
        job_postings_fact 
    WHERE 
        job_work_from_home = True)
ORDER BY median_salary;

-- CTEs --

-- Compare how much more (or less) remote roles pay compared to onsite roles for each job title
-- Use a CTE to calculate the medin salary by title and work arrangement then compare those medians


-- final output : 
-- job_title_short, median_salary_remote, median_salary_onsite

-- median salary per job_work_from_home
WITH title_median AS (
    SELECT
        job_title_short,
        CASE
            WHEN job_work_from_home = TRUE THEN 'remote'
            WHEN job_work_from_home = FALSE THEN 'onsite'
        END AS job_work_from_home,
        ROUND(MEDIAN(salary_year_avg),0) AS median_salary
    FROM 
        job_postings_fact
    WHERE
        job_country = 'United States'
    GROUP BY
        job_title_short,
        job_work_from_home
),
work_arrangement_salries AS (
    SELECT
        r.job_title_short,
        r.median_salary AS remote_median_salary,
        o.median_salary AS onsite_median_salary
    FROM title_median AS r
    INNER JOIN title_median AS o
        ON r.job_title_short = o.job_title_short
    WHERE
        r.job_work_from_home = 'remote'
    AND
        o.job_work_from_home = 'onsite'
)
SELECT
    *,
    remote_median_salary - onsite_median_salary AS remote_premium
FROM
    work_arrangement_salries;


-- Existance Filtering --
-- Subquries WHERE EXISTS (Finds matches from the target table in the source table)
-- Subqueries WHERE NOT EXISTS (Keep rows with NO matching in the target table)

-- Example : 

-- Creat a source table using range function --
-- WHERE EXISTS: Keep rows that are in the source table AND the target table
SELECT
* 
FROM RANGE(3) AS source(key);

SELECT
* 
FROM RANGE(2) AS target(key);

SELECT
    *
FROM 
    RANGE(3) AS source(key)
WHERE NOT EXISTS
(
    SELECT 1
    FROM RANGE(2) AS tgt(key)
    WHERE tgt.key = source.key
);

-- Filter for job ids that do not exist in the skills_jobs_dim
USE data_jobs;
SELECT
    *
FROM job_postings_fact as tgt
WHERE NOT EXISTS (
    SELECT 1
    FROM skills_job_dim as src
    WHERE tgt.job_id = src.job_id
);