WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        UPPER(TRIM(full_name)) AS full_name_normalized,
        COALESCE(level, 'N/A') AS level_filled
    FROM employees
    WHERE manager_id IS NOT NULL
      AND full_name LIKE 'G%'
)
SELECT
    id,
    full_name,
    email,
    dept_id,
    job_title,
    salary,
    is_remote,
    full_name_normalized,
    SUM(salary) OVER w_part AS cumulative_salary,
    AVG(salary) OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_6_prior,
    LEAD(salary, 2) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS next_salary
FROM cleaned
WHERE dept_id = 27
WINDOW w_part AS (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
