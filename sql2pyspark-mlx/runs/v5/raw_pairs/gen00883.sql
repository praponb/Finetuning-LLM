WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(level, 1, 3) AS level_prefix
    FROM employees
    WHERE level IS DISTINCT FROM 'L3'
      AND salary BETWEEN 137000 AND 143000
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(job_title) AS job_title_lower,
        DATE_TRUNC('YEAR', hire_date) AS hire_date_period
    FROM filtered
    WHERE hire_date >= DATE '2023-05-19'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(salary, 2) OVER (PARTITION BY job_title ORDER BY salary, id) AS prev_salary,
        MAX(salary) OVER (PARTITION BY level ORDER BY CAST(hire_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS salary_max_7d
    FROM cleaned
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(prev_salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS next_prev_salary,
        LAG(prev_salary) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS prev_prev_salary,
        LEAD(salary_max_7d) OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS next_salary_max_7d,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) AS row_seq
    FROM scored
)
SELECT
    id,
    full_name,
    email,
    manager_id,
    job_title,
    salary,
    level_prefix,
    hire_date_period,
    prev_salary
FROM with_windows
ORDER BY job_title;
