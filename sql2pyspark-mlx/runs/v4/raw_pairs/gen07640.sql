SELECT s.*, d.hire_date AS employee_hire_date, d.email AS employee_email, d.full_name AS employee_full_name FROM employees s LEFT JOIN employees d ON s.manager_id = d.id;
