SELECT *, RANK() OVER (PARTITION BY level, manager_id ORDER BY hire_date DESC) AS rn FROM employees QUALIFY rn = 1
