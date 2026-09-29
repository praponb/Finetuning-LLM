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
    ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary, id) AS row_seq,
    ROW_NUMBER() OVER (ORDER BY hire_date DESC, id) AS row_seq_2,
    PERCENT_RANK() OVER (PARTITION BY job_title, dept_id ORDER BY salary DESC) AS salary_percent_rank
FROM employees
