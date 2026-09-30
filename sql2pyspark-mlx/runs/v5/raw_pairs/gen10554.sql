SELECT
    d.id,
    d.budget,
    d.dept_name,
    d.location
FROM departments d
WHERE d.id IN (SELECT dept_id FROM employees WHERE hire_date BETWEEN '2023-03-01' AND '2023-03-05');
