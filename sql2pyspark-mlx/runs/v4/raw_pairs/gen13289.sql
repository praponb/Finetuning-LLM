SELECT is_remote, COUNT(*) AS num_employees FROM employees GROUP BY is_remote ORDER BY num_employees ASC;
