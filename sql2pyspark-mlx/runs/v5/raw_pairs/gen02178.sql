SELECT id FROM employees EXCEPT ALL SELECT id FROM employees WHERE dept_id IN (14, 16);
