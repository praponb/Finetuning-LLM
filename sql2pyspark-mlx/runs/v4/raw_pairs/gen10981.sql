SELECT d.id, d.budget, t.latest_hire_date
FROM departments d
INNER JOIN (SELECT dept_id, MAX(hire_date) AS latest_hire_date
    FROM employees
    WHERE manager_id IS NULL
    GROUP BY dept_id) t ON d.id = t.dept_id
