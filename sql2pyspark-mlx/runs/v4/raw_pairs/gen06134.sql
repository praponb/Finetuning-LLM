WITH employees_by_department AS (
    SELECT dept_id, MIN(salary) AS min_salary
    FROM employees
    GROUP BY dept_id
)
SELECT departments.dept_name, departments.budget, employees_by_department.min_salary
FROM departments
JOIN employees_by_department ON departments.id = employees_by_department.dept_id
ORDER BY employees_by_department.min_salary DESC, dept_name;
