SELECT id, salary, TRIM(level) AS clean_level, UPPER(TRIM(email)) AS email_normalized FROM employees WHERE manager_id = 26 AND level IN ('L2', 'L5') AND dept_id IN (17, 40)
