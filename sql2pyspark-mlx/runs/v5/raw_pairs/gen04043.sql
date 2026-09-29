SELECT d.id, COUNT(e.id) AS employee_count, ROUND(STDDEV(e.salary), 2) AS stddev_salary
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
GROUP BY d.id
ORDER BY stddev_salary DESC, id ASC
LIMIT 3
