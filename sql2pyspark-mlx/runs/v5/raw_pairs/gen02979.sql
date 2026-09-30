SELECT level, ROUND(AVG(salary), 2) avg_salary, COUNT(DISTINCT salary) unique_depts
FROM employees
WHERE email LIKE '%@yahoo.com'
GROUP BY level
ORDER BY level;
