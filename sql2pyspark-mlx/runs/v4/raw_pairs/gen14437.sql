SELECT d.budget, t.min_salary, t.designer_count
FROM departments d
LEFT JOIN (SELECT dept_id, MIN(salary) AS min_salary, COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.min_salary >= 97000;
