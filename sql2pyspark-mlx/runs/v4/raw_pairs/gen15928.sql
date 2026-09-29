SELECT
    d.id,
    d.budget,
    t.latest_hire_date,
    t.salary_range
FROM departments d
INNER JOIN (SELECT dept_id, MAX(hire_date) AS latest_hire_date, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    WHERE job_title = 'Intern'
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.salary_range >= 69
ORDER BY d.id DESC, latest_hire_date
LIMIT 10;
