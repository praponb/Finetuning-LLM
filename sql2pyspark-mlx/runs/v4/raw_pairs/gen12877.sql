SELECT e.id, e.level, e.hire_date, e.full_name FROM employees e WHERE e.hire_date > '2023-09-03' AND e.dept_id NOT IN (SELECT id FROM departments WHERE dept_name IN ('Sales', 'Legal', 'HR'))
