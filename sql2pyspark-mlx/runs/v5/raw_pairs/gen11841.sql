SELECT location, SUM(CASE WHEN dept_name = 'Marketing' THEN 1 ELSE 0 END) AS marketing_count, SUM(CASE WHEN dept_name = 'Sales' THEN 1 ELSE 0 END) AS sales_count
FROM departments
GROUP BY location
HAVING marketing_count > 73;
