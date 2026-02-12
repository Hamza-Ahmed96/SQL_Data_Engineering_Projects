/*

Question: What are the highest-paying skills for data engineers?
- Calculate the median salary for each skill required in data engineer positions
- Focus on remote positions with specified salaries
- Include skill frequency to identify both salary and demand
- Why? 
Helps identify which skills command the highest compensation while also showing 
how common those skills are, providing a more complete picture for skill development priorities

*/


SELECT
  sd.skills AS skills,
  ROUND(median(jpf.salary_year_avg), 0) AS median_salary,
  COUNT(jpf.salary_year_avg) AS corrected_demand_count
FROM job_postings_fact jpf 
INNER JOIN skills_job_dim sjd ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd ON sjd.skill_id = sd.skill_id
WHERE 
  job_title_short = 'Data Engineer'
AND 
  job_work_from_home = True
GROUP BY 
  sd.skills
HAVING COUNT(jpf.*) > 100
ORDER 
  BY median_salary DESC
LIMIT 25;

/* 
Insights : Data Engineers 
----------
- Highest earners are niche/modern tools : Rust($210K), Terraform ($184K)
and golang ($184K) - specalised skills command a premimum
- Infrastructure and DevOps pay well too : Terragorm, Kubernetes, Ansible, VMware
have massive demand by less than low demans skills like rust or Zoom
- Takeaway : Learning a niche, modern language (Rust, Go) alognside core DE tools
can significantly boost salary
*/

SELECT
  sd.skills AS skills,
  ROUND(median(jpf.salary_year_avg), 0) AS median_salary,
  COUNT(jpf.salary_year_avg) AS corrected_demand_count
FROM job_postings_fact jpf 
INNER JOIN skills_job_dim sjd ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd ON sjd.skill_id = sd.skill_id
WHERE 
  job_title_short = 'Data Scientist'
AND 
  job_work_from_home = True
GROUP BY 
  sd.skills
HAVING COUNT(jpf.*) > 100
ORDER 
  BY median_salary DESC
LIMIT 25;


/* 
Insights : Data Scientists
----------
- Hihgest paying skills are : atlassian ($217K), slack ($175K) and zoom
These are collaborative tools and pay surprisngly well, suggesting leadership roles rather than the
roles themselves. 
- Cloud and big data skills pay well :  Dynamodb ($174K), Terraform ($160K), BigQuery ($150K)
- Deep Learning frameworks : These have high demand and good salary : 
($149K), Tensorflow ($145K)
Takeaway : DS roles that combine ML with cloud infrastrucure or leadership skills earn more than pure ML 
practitioners. 
*/

SELECT
  sd.skills AS skills,
  ROUND(median(jpf.salary_year_avg), 0) AS median_salary,
  COUNT(jpf.salary_year_avg) AS corrected_demand_count
FROM job_postings_fact jpf 
INNER JOIN skills_job_dim sjd ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd ON sjd.skill_id = sd.skill_id
WHERE 
  job_title_short = 'Data Analyst'
AND 
  job_work_from_home = True
GROUP BY 
  sd.skills
HAVING COUNT(jpf.*) > 100
ORDER 
  BY median_salary DESC
LIMIT 25;


/* 
Insights : Data Analysts
---------
- TypeScript is a massive outlier at $445K — likely a small number of highly
specialized hybrid roles, not representative of typical DA salaries.
- Salaries are notably lower overall: Most skills fall in the $100K–$155K
range vs $150K–$210K for DE.
- Engineering-adjacent skills pay more: PySpark ($153K), Kafka ($145K), Scala
($135K), Airflow ($114K) — analysts who can do engineering work earn more.
- Traditional BI tools pay less: DAX ($110K), SSIS ($110K), SQL Server
($100K) have solid demand but lower pay.
- Takeaway: Data Analysts can increase earnings by picking up data
engineering skills (PySpark, Kafka, Scala) rather than staying purely in BI
tools.
*/