SELECT level, dept_id FROM employees EXCEPT DISTINCT SELECT level, dept_id FROM employees WHERE full_name LIKE '%smit%';
