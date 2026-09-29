SELECT
    id,
    level,
    hire_date,
    dept_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS rn,
    SUM(salary) OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary
FROM employees
WHERE full_name IS NOT NULL
