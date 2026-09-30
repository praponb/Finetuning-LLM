SELECT d.id, d.dept_name, d.location, d.budget FROM departments d WHERE d.location = 'Singapore' AND d.id IN (SELECT dept_id FROM employees WHERE full_name LIKE 'N%');
