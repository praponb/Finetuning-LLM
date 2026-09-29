SELECT
    id,
    full_name,
    dept_id,
    salary,
    hire_date,
    is_remote,
    LAG(salary) OVER (PARTITION BY manager_id, dept_id ORDER BY salary, id) AS prev_salary,
    ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY salary, id) AS event_seq
FROM employees;
