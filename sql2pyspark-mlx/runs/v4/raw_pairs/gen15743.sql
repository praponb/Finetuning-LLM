SELECT d.id, d.dept_name, t.min_hire_date
FROM departments d
LEFT JOIN (SELECT dept_id, MIN(hire_date) AS min_hire_date
    FROM employees
    WHERE manager_id IN (20, 30, 40)
    GROUP BY dept_id) t ON d.id = t.dept_id;
