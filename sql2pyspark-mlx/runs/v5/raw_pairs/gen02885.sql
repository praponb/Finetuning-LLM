SELECT
    e.job_title AS employee_job_title,
    src.job_title AS employee_job_title,
    COUNT(*) AS cnt,
    MIN(src.salary) AS min_salary,
    GROUPING_ID() AS level_id
FROM employees src
LEFT JOIN employees e ON src.manager_id = e.id
GROUP BY ROLLUP (e.job_title, src.job_title);
