SELECT MAX(hire_date) AS latest_hire_date, ROUND((100 * SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l5 FROM employees WHERE dept_id IN (15, 19, 39) AND level IN ('L5', 'L3')
