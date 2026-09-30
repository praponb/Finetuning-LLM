WITH DepartmentStats AS (
    SELECT dept_id, MAX(salary) AS max_salary, MAX(hire_date) AS latest_hire_date
    FROM employees
    WHERE LOWER(full_name) LIKE '%ia %'
    GROUP BY dept_id
)
SELECT
    departments.dept_name,
    departments.budget,
    DepartmentStats.max_salary,
    DepartmentStats.latest_hire_date
FROM departments
JOIN DepartmentStats ON departments.id = DepartmentStats.dept_id
WHERE DepartmentStats.max_salary >= 141000;
