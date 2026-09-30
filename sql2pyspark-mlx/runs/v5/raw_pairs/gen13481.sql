SELECT dept_name, COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count FROM departments GROUP BY dept_name ORDER BY dept_name LIMIT 10;
