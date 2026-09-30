SELECT MONTH(hire_date) AS hire_month, SUM(CASE WHEN level = 'L1' THEN 1 ELSE 0 END) AS l1_count FROM employees GROUP BY 1 ORDER BY hire_month DESC;
