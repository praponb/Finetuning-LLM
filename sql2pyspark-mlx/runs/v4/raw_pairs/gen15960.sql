SELECT
    d.id,
    d.dept_name,
    d.budget,
    t.min_hire_date
FROM departments d
LEFT OUTER JOIN (SELECT dept_id, MIN(hire_date) min_hire_date
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id;
