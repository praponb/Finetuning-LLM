WITH employees_by_department AS (
    SELECT dept_id, MAX(salary) AS max_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.budget,
    departments.dept_name,
    employees_by_department.max_salary
FROM departments
INNER JOIN employees_by_department ON departments.id = employees_by_department.dept_id
ORDER BY departments.budget DESC;
