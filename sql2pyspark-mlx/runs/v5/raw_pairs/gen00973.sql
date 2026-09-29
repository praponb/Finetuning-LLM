SELECT
    id,
    hire_date,
    salary,
    dept_id,
    salary - LAG(salary, 2) OVER (ORDER BY hire_date, id) AS salary_change,
    MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary
FROM employees;
