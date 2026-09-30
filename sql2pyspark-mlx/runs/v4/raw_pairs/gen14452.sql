WITH EmployeesByDepartment AS (
    SELECT dept_id, MIN(salary) min_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    d.location,
    E.min_salary
FROM departments d
INNER JOIN EmployeesByDepartment E ON d.id = E.dept_id
WHERE E.min_salary >= 126000;
