SELECT
    id,
    email,
    salary,
    DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date,
    CASE WHEN salary >= 149000 THEN 'High' WHEN salary >= 55000 THEN 'Medium' ELSE 'Low' END AS salary_level
FROM employees
ORDER BY id, salary DESC
