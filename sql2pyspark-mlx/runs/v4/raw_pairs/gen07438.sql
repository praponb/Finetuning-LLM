SELECT
    id,
    dept_id,
    salary,
    job_title,
    MIN(salary) OVER (PARTITION BY dept_id) dept_id_min_salary,
    ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) rn
FROM employees;
