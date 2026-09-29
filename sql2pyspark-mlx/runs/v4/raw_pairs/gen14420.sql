SELECT id, dept_name
FROM departments
EXCEPT ALL
SELECT id, dept_name
FROM departments
WHERE location IS NOT NULL;
