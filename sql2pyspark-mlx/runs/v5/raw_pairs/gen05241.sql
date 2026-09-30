SELECT *, INITCAP(TRIM(level)) level_title FROM employees WHERE dept_id IN (16, 21, 31, 34) OR dept_id IN (22, 25, 38);
