SELECT dept_name, location
FROM departments
EXCEPT DISTINCT
SELECT dept_name, location
FROM departments
WHERE dept_name IN ('Legal', 'Finance', 'HR')
