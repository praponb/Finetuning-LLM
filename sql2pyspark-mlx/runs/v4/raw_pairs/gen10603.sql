SELECT CASE WHEN d.budget >= 4038000 THEN 'high' WHEN d.budget >= 1469000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN e.job_title = 'Intern' THEN 1 ELSE 0 END) AS intern_count, MIN(e.salary) AS min_salary
FROM employees e
LEFT JOIN departments d ON d.id = e.dept_id
GROUP BY CASE WHEN d.budget >= 4038000 THEN 'high' WHEN d.budget >= 1469000 THEN 'mid' ELSE 'low' END
ORDER BY budget_bucket
