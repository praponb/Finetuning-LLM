SELECT e.id, e.job_title AS employee_job_title, m.email AS manager_email FROM employees e LEFT JOIN employees m ON e.manager_id = m.id;
