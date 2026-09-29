SELECT id, dept_name
FROM departments
WHERE dept_name IN ('Finance', 'Legal')
UNION ALL
SELECT id, dept_name
FROM departments
WHERE budget > 1761000
ORDER BY dept_name
