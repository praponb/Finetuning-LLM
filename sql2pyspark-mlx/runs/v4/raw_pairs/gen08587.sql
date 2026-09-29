SELECT d.id, d.location, t.total_count
FROM departments d
LEFT JOIN (SELECT dept_id, COUNT(*) AS total_count
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.total_count >= 5;
