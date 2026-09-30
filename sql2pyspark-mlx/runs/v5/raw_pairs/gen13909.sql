SELECT
    d.location,
    COUNT(e.id) AS employee_count,
    MIN(e.salary) AS min_salary,
    ROUND((100 * SUM(CASE WHEN e.level = 'L1' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l1
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE e.salary BETWEEN 87000 AND 127000
GROUP BY d.location;
