SELECT dept_name, SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) AS engineering_count
FROM departments
WHERE budget <= 2029000 AND (location <> 'Chiang Mai' OR dept_name = 'HR')
GROUP BY dept_name
HAVING SUM(CASE WHEN dept_name = 'Engineering' THEN 1 ELSE 0 END) > 60
ORDER BY dept_name DESC
