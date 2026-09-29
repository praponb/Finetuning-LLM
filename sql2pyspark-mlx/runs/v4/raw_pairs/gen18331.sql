SELECT
    d.id,
    d.location,
    d.budget,
    d.dept_name
FROM departments d
WHERE d.budget <= 3742000 AND d.id IN (SELECT dept_id FROM employees WHERE level IN ('L5', 'L4', 'L3'))
ORDER BY budget DESC, d.location;
