SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, COUNT(DISTINCT salary) AS unique_levels, COUNT(*) AS cnt FROM employees GROUP BY 1 ORDER BY hire_month;
