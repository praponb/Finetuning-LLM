WITH employees_by_department AS (
    SELECT dept_id, MAX(salary) AS max_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, d.location, ebd.max_salary
FROM departments d
INNER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.max_salary > 121000;
