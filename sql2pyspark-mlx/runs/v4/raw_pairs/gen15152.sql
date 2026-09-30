SELECT
    dept_name,
    location,
    SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) engineering_count,
    COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END) finance_count
FROM departments
WHERE location = 'Tokyo'
GROUP BY dept_name, location
ORDER BY location
