/*
Question: What are the most optimal skills for data engineers—balancing both demand and salary?
- Create a ranking column that combines demand count and median salary to identify the most valuable skills.
- Focus only on remote Data Engineer positions with specified annual salaries.
- Why?
    - This approach highlights skills that balance market demand and financial reward. It weights core skills appropriately instead of letting rare, outlier skills distort the results.
    - The natural log transformation ensures that both high-salary and widely in-demand skills surface as the most practical and valuable to learn for data engineering careers.
*/

/* 
Methodlogy:
----------

We need to create a ranking system that helps us answer the question : 
**What is the most optiaml skill to have as a Data Engineer/Scientist/Analyst**
For this we need to develop a scoring system that combines the median salary associated with a skill
and the demand for that skill. 

One method we can try is simply multiplying the median salary with the demand and then ranking the tables
with this new score. However, this posses a problem :
When you multiply salary by demand directly:
  ┌─────────┬──────────┬────────┬─────────────────────────┐
  │  Skill  │  Salary  │ Demand │ Score (salary × demand) │
  ├─────────┼──────────┼────────┼─────────────────────────┤
  │ Airflow │ $150,000 │ 9,996  │ 1,499,400,000           │
  ├─────────┼──────────┼────────┼─────────────────────────┤
  │ Rust    │ $210,000 │ 232    │ 48,720,000              │
  └─────────┴──────────┴────────┴─────────────────────────┘
Airflow's score is 30x higher than Rust's, even though Rust pays $60K more.
The demand difference (43x) completely drowns out the salary difference
(1.4x). Demand dominates because its range (100–10,000) is much wider relative
to salary's range ($100K–$210K).

Therefore, instead we use the Natual log (ln) why ?
- This depends on the type of data that we have, plotting skills against 
demand shows an exponential growth, hence why the demand collumn is overwhelming
the ranking. 
The fix here is to transform the demand column then using the natual log, which 
compresses large numbers more than small ones. It shrinks the scale while preserving 
the rankig ;


Natural log compresses large numbers more than small ones. It shrinks the
scale while preserving the ranking:
┌────────┬────────────┐
│ Demand │ ln(Demand) │
├────────┼────────────┤
│ 100    │ 4.6        │
├────────┼────────────┤
│ 500    │ 6.2        │
├────────┼────────────┤
│ 1,000  │ 6.9        │
├────────┼────────────┤
│ 5,000  │ 8.5        │
├────────┼────────────┤
│ 10,000 │ 9.2        │
└────────┴────────────┘
Notice: going from 100 → 1,000 (10x increase) only adds 2.3 to the log value.
Going from 1,000 → 10,000 (another 10x) only adds another 2.3. This is because
log grows proportionally — each doubling adds the same fixed amount.

The Fix in Action

Now multiply salary by ln(demand):
┌─────────┬──────────┬────────┬────────────┬─────────────────────┐
│  Skill  │  Salary  │ Demand │ ln(Demand) │ Score (salary × ln) │
├─────────┼──────────┼────────┼────────────┼─────────────────────┤
│ Airflow │ $150,000 │ 9,996  │ 9.2        │ 1,380,000           │
├─────────┼──────────┼────────┼────────────┼─────────────────────┤
│ Rust    │ $210,000 │ 232    │ 5.4        │ 1,134,000           │
└─────────┴──────────┴────────┴────────────┴─────────────────────┘
Now the scores are comparable. Airflow is still ahead (it has both good pay
and high demand), but Rust isn't buried — its high salary actually matters in
  the ranking.
*/

SELECT
  sd.skills AS skills,
  ROUND(median(jpf.salary_year_avg), 0) AS median_salary,
  COUNT(jpf.salary_year_avg) AS corrected_demand_count,
  ROUND((LN(COUNT(jpf.salary_year_avg)) * MEDIAN(jpf.salary_year_avg)/1_000_000),2) AS optimal_score
FROM job_postings_fact jpf 
INNER JOIN skills_job_dim sjd ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd ON sjd.skill_id = sd.skill_id
WHERE 
  job_title_short = 'Data Engineer'
AND 
  job_work_from_home = True
AND
    jpf.salary_year_avg IS NOT NULL
GROUP BY 
  sd.skills
HAVING COUNT(jpf.*) > 100
ORDER 
  BY optimal_score DESC
LIMIT 25;

SELECT
  sd.skills AS skills,
  ROUND(median(jpf.salary_year_avg), 0) AS median_salary,
  COUNT(jpf.salary_year_avg) AS corrected_demand_count,
  ROUND((LN(COUNT(jpf.salary_year_avg)) * MEDIAN(jpf.salary_year_avg)/1_000_000),2) AS optimal_score
FROM job_postings_fact jpf 
INNER JOIN skills_job_dim sjd ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd ON sjd.skill_id = sd.skill_id
WHERE 
  job_title_short = 'Data Scientist'
AND 
  job_work_from_home = True
AND
    jpf.salary_year_avg IS NOT NULL
GROUP BY 
  sd.skills
HAVING COUNT(jpf.*) > 100
ORDER 
  BY optimal_score DESC
LIMIT 25;

/*
insights: Data Scientists
-------------------------

The Core Foundation 

- Python, SQL and R are non-negiotable. They top the demand and provide 
a solid salary. 

Visulisations

- Tableau and aQS sit at the same score. You're expected to present findings 
and work in cloud environment

Deep Learning is Where salary Premium Lives
- TensorFlow and PyTorch have lower demand but pay noticeably more. 


 The "Data Engineer Lite" Tier (0.71–0.79)

  - Spark, Pandas, Hadoop, Snowflake, Scikit-learn — these signal you can handle
   data at scale and build production models, not just notebooks. Employers
  increasingly want DS who can do some engineering.

  Notable Gaps

  - Jupyter is an extreme outlier — high demand (116) but only $74K median
  salary. It's likely correlated with junior/entry-level roles. Everyone uses
  it, so it provides zero competitive advantage.
  - Excel and Power BI sit near the bottom (~$121–122K). They're useful but
  won't differentiate you from analysts.
*/
SELECT
  sd.skills AS skills,
  ROUND(median(jpf.salary_year_avg), 0) AS median_salary,
  COUNT(jpf.salary_year_avg) AS corrected_demand_count,
  ROUND((LN(COUNT(jpf.salary_year_avg)) * MEDIAN(jpf.salary_year_avg)/1_000_000),2) AS optimal_score
FROM job_postings_fact jpf 
INNER JOIN skills_job_dim sjd ON jpf.job_id = sjd.job_id
INNER JOIN skills_dim sd ON sjd.skill_id = sd.skill_id
WHERE 
  job_title_short = 'Data Analyst'
AND 
  job_work_from_home = True
AND
    jpf.salary_year_avg IS NOT NULL
GROUP BY 
  sd.skills
HAVING COUNT(jpf.*) > 100
ORDER 
  BY optimal_score DESC
LIMIT 25;
