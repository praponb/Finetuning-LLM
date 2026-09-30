SELECT
    id,
    manager_id,
    dept_id,
    salary
FROM employees
WHERE email LIKE '%@gmail.com'
ORDER BY salary;
