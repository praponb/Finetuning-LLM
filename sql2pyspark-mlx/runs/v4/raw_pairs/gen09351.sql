SELECT e.id, e.email, e.hire_date, e.dept_id, d.dept_name FROM employees e JOIN departments d ON d.id = e.dept_id;
