SELECT d.id, d.dept_name, COUNT(e.id) AS employee_count
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
WHERE e.salary < 116000
GROUP BY d.id, d.dept_name
ORDER BY id DESC, dept_name
LIMIT 5
