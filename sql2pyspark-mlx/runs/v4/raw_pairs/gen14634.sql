SELECT id, dept_name, location
FROM departments
WHERE location NOT IN ('Chiang Mai', 'Tokyo', 'Bangkok')
EXCEPT
SELECT id, dept_name, location
FROM departments
WHERE location NOT IN ('Chiang Mai', 'Singapore')
