SELECT dept_id, level
FROM employees
INTERSECT
SELECT dept_id, level
FROM employees
WHERE dept_id IN (15, 25, 38)
