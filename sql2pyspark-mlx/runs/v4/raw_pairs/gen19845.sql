SELECT id, location, dept_name
FROM departments
WHERE budget >= 2545000
EXCEPT
SELECT id, location, dept_name
FROM departments
WHERE location NOT IN ('Tokyo', 'Singapore')
ORDER BY location;
