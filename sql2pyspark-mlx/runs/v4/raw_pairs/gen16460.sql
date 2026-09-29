SELECT d.location, COUNT(e.id) AS employee_count, ROUND(STDDEV(e.salary), 2) AS stddev_salary
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
GROUP BY d.location
ORDER BY location;
