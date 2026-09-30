SELECT e.id, e.dept_id employee_dept_id, e.is_remote employee_is_remote, m.dept_id manager_dept_id FROM employees e LEFT OUTER JOIN employees m ON e.manager_id = m.id WHERE e.job_title = 'Intern';
