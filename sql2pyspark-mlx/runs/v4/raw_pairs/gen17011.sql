SELECT level, SUM(CASE WHEN level = 'L5' THEN salary ELSE 0 END) AS l5_salary FROM employees GROUP BY level;
