SELECT e.id, e.is_remote FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE location NOT IN ('New York', 'Tokyo', 'Chiang Mai'));
