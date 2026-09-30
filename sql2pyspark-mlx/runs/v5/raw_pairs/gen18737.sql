SELECT location, COUNT(*) AS row_count
FROM departments
WHERE dept_name IN ('Sales', 'Finance')
GROUP BY location
HAVING COUNT(*) >= 2
ORDER BY location;
