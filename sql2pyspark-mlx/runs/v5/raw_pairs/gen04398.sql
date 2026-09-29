SELECT s.* FROM departments s WHERE NOT EXISTS (SELECT 1 FROM employees x WHERE x.dept_id = s.id AND x.hire_date BETWEEN '2025-05-01' AND '2025-09-01');
