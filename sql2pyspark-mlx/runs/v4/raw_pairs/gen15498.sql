SELECT
    dept_id,
    manager_id,
    hire_date,
    ROUND(salary * 100, 2) adjusted_salary
FROM employees
WHERE manager_id IS NULL AND (full_name LIKE '%For%' OR email LIKE '%@startup.io')
