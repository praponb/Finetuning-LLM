SELECT
    e.level AS employee_level,
    src.job_title AS employee_job_title,
    COUNT(*) AS cnt,
    MIN(src.salary) AS min_salary,
    GROUPING_ID() AS gid
FROM employees src
INNER JOIN employees e ON src.manager_id = e.id
GROUP BY GROUPING SETS ((e.level, src.job_title), (e.level))
ORDER BY employee_job_title NULLS FIRST;
