SELECT d.id, COUNT(e.id) AS employee_count, AVG(e.salary) AS avg_salary
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.budget BETWEEN 3092000 AND 3914000
GROUP BY d.id
ORDER BY avg_salary
