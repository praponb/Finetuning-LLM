SELECT CASE WHEN salary >= 130000 THEN 'high' WHEN salary >= 44000 THEN 'mid' ELSE 'low' END AS salary_bucket, ROUND(AVG(salary), 2) AS avg_salary, MAX(salary) AS max_salary
FROM employees
GROUP BY 1
