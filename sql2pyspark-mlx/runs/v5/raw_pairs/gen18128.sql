SELECT CASE WHEN d.budget >= 3671000 THEN 'high' WHEN d.budget >= 1139000 THEN 'mid' ELSE 'low' END AS budget_bucket, SUM(CASE WHEN e.level = 'L2' THEN e.salary ELSE 0 END) AS l2_salary, SUM(CASE WHEN e.level = 'L4' THEN e.salary ELSE 0 END) AS l4_salary
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
WHERE e.is_remote = TRUE
GROUP BY CASE WHEN d.budget >= 3671000 THEN 'high' WHEN d.budget >= 1139000 THEN 'mid' ELSE 'low' END
HAVING l2_salary >= 49
ORDER BY l2_salary;
