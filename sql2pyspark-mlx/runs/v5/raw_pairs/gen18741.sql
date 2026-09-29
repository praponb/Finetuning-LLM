SELECT
    id,
    full_name,
    email,
    dept_id,
    job_title,
    hire_date,
    is_remote,
    level,
    LEAD(salary) OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC) AS next_salary,
    LAG(salary) OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC) AS prev_salary,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date ASC, id ASC) AS event_seq
FROM employees;
