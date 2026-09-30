SELECT e.dept_id employee_dept_id, m.is_remote manager_is_remote, m.hire_date manager_hire_date FROM employees e JOIN employees m ON e.manager_id = m.id ORDER BY manager_hire_date;
