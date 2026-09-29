SELECT
    id,
    manager_id,
    salary,
    MIN(salary) OVER (PARTITION BY manager_id) AS manager_id_min_salary
FROM employees
ORDER BY id;
