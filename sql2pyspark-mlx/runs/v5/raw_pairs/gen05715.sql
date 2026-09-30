SELECT
    full_name,
    level,
    dept_id,
    ROUND(salary, 2) AS salary_rounded
FROM employees
WHERE MONTH(hire_date) IN (4, 5, 6) AND LENGTH(email) < 11
