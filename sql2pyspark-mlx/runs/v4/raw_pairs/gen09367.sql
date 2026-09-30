SELECT
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    LEAD(salary, 1) OVER w_running AS next_salary,
    ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share,
    ROW_NUMBER() OVER w_recent AS seq_no,
    FIRST_VALUE(level) IGNORE NULLS OVER w_hist AS first_known_level
FROM employees
WINDOW w_running AS (PARTITION BY dept_id, manager_id ORDER BY hire_date, id), w_recent AS (PARTITION BY dept_id ORDER BY salary, id), w_hist AS (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
