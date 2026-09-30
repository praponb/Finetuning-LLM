SELECT
    level,
    COUNT(*) num_employees,
    MIN(hire_date) min_hire_date,
    SUM(salary) total_salary
FROM employees
WHERE LENGTH(email) >= 5 AND job_title IS NOT NULL
GROUP BY 1
HAVING COUNT(DISTINCT salary) >= 2
ORDER BY total_salary
