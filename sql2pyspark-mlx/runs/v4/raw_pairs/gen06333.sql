SELECT
    d.id,
    d.dept_name,
    t.pct_l3,
    t.designer_count
FROM departments d
INNER JOIN (SELECT dept_id, ROUND((100 * SUM(CASE WHEN level = 'L3' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l3, SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END) AS designer_count
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.pct_l3 > 44
ORDER BY d.id DESC;
