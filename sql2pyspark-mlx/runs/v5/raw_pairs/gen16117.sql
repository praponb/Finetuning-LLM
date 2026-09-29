WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        CEIL(salary) AS salary_whole
    FROM employees
    WHERE level IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        salary_whole + salary AS salary_whole_plus_salary,
        COALESCE(level, 'NONE') AS level_filled
    FROM base
    WHERE hire_date > DATE '2023-06-08'
      AND job_title IS NOT NULL
),
base_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        job_title,
        salary,
        hire_date,
        level,
        salary_whole,
        salary_whole_plus_salary,
        level_filled,
        SUBSTRING(full_name, 1, 2) AS full_name_prefix
    FROM filtered
    WHERE salary_whole_plus_salary IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(salary) OVER (PARTITION BY job_title ORDER BY salary_whole_plus_salary, id) AS prev_salary,
        LEAD(salary_whole_plus_salary) OVER (PARTITION BY job_title ORDER BY salary_whole_plus_salary, id) AS next_salary_whole_plus_salary,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary_whole_plus_salary, id) AS event_seq
    FROM base_employees
)
-- Running and moving aggregates
SELECT
    id,
    salary,
    hire_date,
    level_filled,
    full_name_prefix,
    next_salary_whole_plus_salary,
    AVG(salary_whole_plus_salary) OVER (PARTITION BY job_title ORDER BY salary_whole, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_whole_plus_salary_avg_5_prior,
    LAST_VALUE(salary_whole, TRUE) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_salary_whole
FROM scored;
