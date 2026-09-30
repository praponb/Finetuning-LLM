SELECT e.id, e.hire_date, e.full_name FROM employees e WHERE e.dept_id IN (10, 15, 22) AND e.dept_id NOT IN (SELECT id FROM departments WHERE budget BETWEEN 1802000 AND 3891000) ORDER BY e.full_name
