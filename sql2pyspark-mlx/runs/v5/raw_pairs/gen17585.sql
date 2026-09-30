SELECT DISTINCT dept_name, location
FROM departments
WHERE location NOT IN ('New York', 'Singapore', 'Bangkok');
