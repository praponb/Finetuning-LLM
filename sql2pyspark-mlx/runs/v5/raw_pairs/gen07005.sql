SELECT location
FROM departments
WHERE dept_name IN ('Marketing', 'HR')
UNION ALL
SELECT location
FROM departments
WHERE location = 'Chiang Mai'
ORDER BY location;
