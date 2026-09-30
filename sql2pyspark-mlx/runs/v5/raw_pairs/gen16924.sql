SELECT d.id, d.budget, d.dept_name
FROM departments d
WHERE d.location IN ('Chiang Mai', 'Singapore') AND d.id IN (SELECT dept_id FROM employees)
ORDER BY d.id DESC
LIMIT 5;
