WITH employees_by_department AS (
    SELECT dept_id, COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END) AS analyst_count
    FROM employees
    GROUP BY dept_id
)
SELECT d.id, d.location, ebd.analyst_count
FROM departments d
LEFT JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.analyst_count > 10
ORDER BY d.location DESC, ebd.analyst_count DESC;
