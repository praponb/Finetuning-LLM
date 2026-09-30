SELECT d.id, d.dept_name, d.budget FROM departments d WHERE d.budget BETWEEN 1268000 AND 4007000 AND d.id IN (SELECT dept_id FROM employees) ORDER BY d.budget DESC
