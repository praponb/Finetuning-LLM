SELECT id, email, dept_id, manager_id, job_title, salary, hire_date, level FROM employees QUALIFY RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) = 1;
