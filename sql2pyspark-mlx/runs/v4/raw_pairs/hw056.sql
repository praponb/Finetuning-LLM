SELECT dept_id, SUM(CASE WHEN is_remote THEN 1 ELSE 0 END) AS remote_count FROM employees GROUP BY dept_id;
