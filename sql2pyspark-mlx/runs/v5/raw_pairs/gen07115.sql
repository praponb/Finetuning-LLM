SELECT d.budget, d.dept_name, t.director_count
FROM departments d
JOIN (SELECT dept_id, COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count
    FROM employees
    WHERE hire_date >= '2025-04-01'
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.director_count >= 10
ORDER BY d.dept_name, director_count;
