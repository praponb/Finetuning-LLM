SELECT * FROM employees QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id, level ORDER BY hire_date DESC, id ASC) = 1;
