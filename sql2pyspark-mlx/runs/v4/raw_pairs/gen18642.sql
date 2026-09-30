WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        is_remote,
        level,
        UPPER(full_name) AS full_name_upper,
        CONCAT_WS('-', email, level) AS email_level_key
    FROM employees
    WHERE level NOT IN ('L3', 'L4', 'L5')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior
    FROM base
),
base_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        salary,
        is_remote,
        level,
        full_name_upper,
        email_level_key,
        salary_share,
        LOWER(full_name) AS full_name_lower
    FROM scored
    WHERE dept_id IN (14, 30)
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        salary,
        is_remote,
        level,
        full_name_upper,
        email_level_key,
        salary_share,
        full_name_lower,
        MIN(salary) OVER (PARTITION BY level) AS level_min_salary,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY dept_id, level ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        MIN(salary) OVER (PARTITION BY dept_id) AS dept_id_min_salary,
        LAST_VALUE(level, TRUE) OVER (ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level_2,
        LAST_VALUE(level) IGNORE NULLS OVER (ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level_3
    FROM base_employees
)
SELECT
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.salary,
    s.level,
    s.full_name_upper,
    s.email_level_key,
    s.salary_share,
    (SELECT MAX(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS max_employee_salary
FROM scored_employees s
ORDER BY salary DESC, dept_id
