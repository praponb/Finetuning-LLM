SELECT CASE WHEN d.budget >= 3093000 THEN 'high' WHEN d.budget >= 1352000 THEN 'mid' ELSE 'low' END AS budget_bucket, MAX(e.salary) AS max_salary, MAX(e.hire_date) AS latest_hire_date
FROM employees e
LEFT OUTER JOIN departments d ON e.dept_id = d.id
GROUP BY CASE WHEN d.budget >= 3093000 THEN 'high' WHEN d.budget >= 1352000 THEN 'mid' ELSE 'low' END
ORDER BY max_salary ASC, latest_hire_date ASC, budget_bucket ASC
LIMIT 3;
