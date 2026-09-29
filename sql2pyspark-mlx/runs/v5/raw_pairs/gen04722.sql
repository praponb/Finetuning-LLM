WITH cleaned AS (
    SELECT
        *,
        CASE WHEN salary >= 155000 THEN 'High' WHEN salary >= 86000 THEN 'Medium' ELSE 'Low' END AS salary_tier,
        ROUND(salary, 2) AS salary_rounded,
        COALESCE(job_title, 'OTHER') AS job_title_clean
    FROM employees
    WHERE LOWER(email) LIKE '%ack%'
),
cleaned_employees AS (
    SELECT
        id,
        full_name,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        salary_tier,
        salary_rounded,
        job_title_clean,
        LAST_DAY(hire_date) AS hire_date_month_end
    FROM cleaned
    WHERE hire_date > DATE '2023-05-12'
      AND job_title IS NOT NULL
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY salary DESC) AS salary_dense_rank,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS salary_avg_2_prior
    FROM cleaned_employees
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY salary) AS salary_cume_dist,
        NTILE(5) OVER (PARTITION BY job_title ORDER BY salary_dense_rank, id) AS salary_dense_rank_bucket,
        CUME_DIST() OVER (ORDER BY salary_rounded) AS salary_rounded_cume_dist
    FROM enriched
)
SELECT
    CASE WHEN salary_rounded >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_rounded_tier,
    level,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    STDDEV_SAMP(salary) AS salary_stddev,
    SUM(CASE WHEN job_title = 'Intern' THEN salary_avg_2_prior ELSE 0 END) AS intern_salary_avg_2_prior
FROM metrics
GROUP BY CASE WHEN salary_rounded >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END, level
HAVING intern_salary_avg_2_prior < 1000000
ORDER BY salary_rounded_tier DESC NULLS FIRST
