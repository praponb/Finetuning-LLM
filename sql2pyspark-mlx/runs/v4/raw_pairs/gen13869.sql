WITH DepartmentSummary AS (
    SELECT dept_id, MIN(salary) AS min_salary, MAX(hire_date) AS latest_hire_date
    FROM employees
    GROUP BY dept_id
)
SELECT
    d.id,
    d.budget,
    D.min_salary,
    D.latest_hire_date
FROM departments d
JOIN DepartmentSummary D ON d.id = D.dept_id;
