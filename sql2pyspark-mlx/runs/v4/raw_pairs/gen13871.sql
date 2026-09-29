SELECT dept_name, COUNT(*) row_count FROM departments GROUP BY dept_name HAVING COUNT(budget) > 3 ORDER BY dept_name LIMIT 10;
