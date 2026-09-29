SELECT dept_name, location
FROM departments
WHERE budget >= 2612000
UNION
SELECT dept_name, location
FROM departments
WHERE budget >= 1172000;
