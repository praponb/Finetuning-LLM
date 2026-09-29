SELECT
    id,
    dept_id,
    hire_date,
    salary,
    LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS prev_salary
FROM employees;
