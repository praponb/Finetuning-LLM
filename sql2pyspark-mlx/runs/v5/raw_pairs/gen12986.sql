SELECT
    id,
    dept_id,
    manager_id,
    ROUND(salary * 100, 2) salary_scaled
FROM employees
WHERE email NOT LIKE '%@company.co';
