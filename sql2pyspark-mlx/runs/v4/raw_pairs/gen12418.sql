SELECT CASE WHEN d.budget >= 3981000 THEN 'high' WHEN d.budget >= 1031000 THEN 'mid' ELSE 'low' END AS budget_bucket, MIN(e.hire_date) AS first_hire_date, AVG(e.salary) AS avg_salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id
WHERE e.is_remote = FALSE
GROUP BY CASE WHEN d.budget >= 3981000 THEN 'high' WHEN d.budget >= 1031000 THEN 'mid' ELSE 'low' END
ORDER BY avg_salary DESC, budget_bucket ASC
LIMIT 20
