SELECT salary, dept_id, hire_date, CAST(is_remote AS INT) AS is_remote_flag, CASE WHEN level = 'L3' THEN 1 ELSE 0 END AS is_l3 FROM employees WHERE dept_id IN (5, 23, 38)
