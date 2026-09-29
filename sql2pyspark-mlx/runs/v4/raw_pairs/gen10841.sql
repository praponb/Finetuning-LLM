SELECT CASE WHEN d.budget >= 3447000 THEN 'high' WHEN d.budget >= 1700000 THEN 'mid' ELSE 'low' END AS budget_bucket, ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE d.location IN ('Berlin', 'London')
GROUP BY CASE WHEN d.budget >= 3447000 THEN 'high' WHEN d.budget >= 1700000 THEN 'mid' ELSE 'low' END
ORDER BY 1
