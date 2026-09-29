SELECT
    MONTH(hire_date) AS hire_month,
    COUNT(*) AS num_employees,
    MAX(salary) AS max_salary,
    ROUND(AVG(salary), 2) AS avg_salary
FROM employees
WHERE email IS NOT NULL
GROUP BY MONTH(hire_date)
HAVING ROUND(AVG(salary), 2) > 52
ORDER BY num_employees
