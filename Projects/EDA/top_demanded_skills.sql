/*
Question: What are the most in-demand skills for data engineers?
- Join job postings to inner join table similar to query 2
- Identify the top 10 in-demand skills for data engineers
- Focus on remote job postings
- Why? Retrieves the top 10 skills with the highest demand in the remote job market,
    providing insights into the most valuable skills for data engineers seeking remote work
*/

-- Data Engineer 
SELECT
sd.skills,
COUNT(sd.skills) AS skill_count
FROM job_postings_fact jpf
INNER JOIN skills_job_dim sjd
ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd
ON sjd.skill_id = sd.skill_id
WHERE job_title_short = 'Data Engineer'
AND jpf.job_work_from_home = True
GROUP BY sd.skills
ORDER BY skill_count DESC
LIMIT 10;



-- Data Scientist
SELECT
sd.skills,
COUNT(sd.skills) AS skill_count
FROM job_postings_fact jpf
INNER JOIN skills_job_dim sjd
ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd
ON sjd.skill_id = sd.skill_id
WHERE job_title_short = 'Data Scientist'
--AND jpf.job_work_from_home = True
GROUP BY sd.skills
ORDER BY skill_count DESC
LIMIT 10;

-- Data Analyst 
SELECT
sd.skills,
COUNT(sd.skills) AS skill_count
FROM job_postings_fact jpf
INNER JOIN skills_job_dim sjd
ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd
ON sjd.skill_id = sd.skill_id
WHERE job_title_short = 'Data Analyst'
GROUP BY sd.skills
ORDER BY skill_count DESC
LIMIT 10;
