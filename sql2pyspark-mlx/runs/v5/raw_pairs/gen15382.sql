SELECT is_remote, MIN(hire_date) AS min_hire_date FROM employees GROUP BY is_remote ORDER BY min_hire_date DESC;
