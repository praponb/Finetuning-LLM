SELECT
    id,
    hire_date,
    dept_id,
    salary
FROM employees
WHERE hire_date = (SELECT MAX(hire_date) FROM employees WHERE email LIKE '%@mail.co.th');
