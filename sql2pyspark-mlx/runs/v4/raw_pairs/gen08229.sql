SELECT *, SUBSTRING(dept_name, 1, 2) AS dept_name_prefix
FROM departments
WHERE dept_name = 'HR' OR budget >= 3573000
ORDER BY location
