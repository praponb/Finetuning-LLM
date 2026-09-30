SELECT
    d.location,
    src.job_title,
    COUNT(*) AS rows_total,
    SUM(src.salary) AS total_salary,
    GROUPING_ID() AS gid
FROM employees src
INNER JOIN departments d ON src.dept_id = d.id
GROUP BY GROUPING SETS ((d.location, src.job_title), (d.location), ());
