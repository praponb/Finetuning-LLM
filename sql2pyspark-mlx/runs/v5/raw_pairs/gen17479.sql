SELECT
    id,
    dept_id,
    hire_date,
    salary,
    LEAD(salary, 1) OVER (PARTITION BY dept_id ORDER BY hire_date, id) AS next_salary
FROM employees;
