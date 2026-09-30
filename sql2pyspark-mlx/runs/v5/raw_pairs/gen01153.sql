SELECT dept_name
FROM departments
WHERE location IS NULL
INTERSECT
SELECT dept_name
FROM departments
WHERE dept_name IS NULL
