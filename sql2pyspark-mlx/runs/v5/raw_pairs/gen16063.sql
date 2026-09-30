SELECT
    d.level AS employee_level,
    s.job_title AS employee_job_title,
    COUNT(*) AS cnt,
    MIN(s.salary) AS min_salary,
    GROUPING_ID() AS grouping_level
FROM employees s
JOIN employees d ON s.manager_id = d.id
GROUP BY GROUPING SETS ((d.level, s.job_title), (d.level))
ORDER BY employee_job_title DESC NULLS FIRST;
