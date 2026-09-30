SELECT d.location, COUNT(e.id) AS employee_count, COUNT(CASE WHEN e.job_title = 'Director' THEN 1 END) AS director_count
FROM departments d
JOIN employees e ON d.id = e.dept_id
GROUP BY d.location;
