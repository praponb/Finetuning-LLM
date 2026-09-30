SELECT
    *,
    NTILE(3) OVER (PARTITION BY manager_id, job_title ORDER BY salary, id) salary_bucket,
    ROW_NUMBER() OVER (PARTITION BY job_title, level ORDER BY hire_date, id) event_seq
FROM employees;
