SELECT e.hire_date, d.dept_name, d.budget
FROM employees e
JOIN departments d ON e.dept_id = d.id
ORDER BY hire_date;
