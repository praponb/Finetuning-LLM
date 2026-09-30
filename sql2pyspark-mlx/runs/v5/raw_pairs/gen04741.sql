WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(full_name, 1, 3) AS full_name_prefix
    FROM employees
    WHERE hire_date >= DATE '2023-03-18'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(TRIM(email)) AS email_normalized
    FROM cleaned
    WHERE hire_date >= DATE '2023-04-20'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS salary_rank,
        RANK() OVER (PARTITION BY level, job_title ORDER BY salary DESC) AS salary_rank_2,
        SUM(salary) OVER (PARTITION BY dept_id, level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
    FROM valid
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.*,
        p.budget,
        p.location,
        p.dept_name
    FROM metrics a
    JOIN departments p
        ON a.dept_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN salary_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_rank_2_tier,
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    SUM(CASE WHEN job_title = 'Director' THEN budget ELSE 0 END) AS director_budget,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    SUM(running_salary) FILTER (WHERE dept_name = 'Legal') AS legal_total_running_salary,
    COUNT(*) FILTER (WHERE job_title = 'Engineer') AS engineer_rows,
    RANK() OVER (PARTITION BY CASE WHEN salary_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SIZE(COLLECT_SET(job_title)) DESC) AS num_job_titles_rank
FROM with_dims
GROUP BY CASE WHEN salary_rank_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', hire_date)
HAVING SUM(CASE WHEN job_title = 'Director' THEN budget ELSE 0 END) > 0;
