SELECT
    is_remote,
    level,
    job_title,
    MIN(hire_date) AS min_hire_date
FROM employees
GROUP BY is_remote, level, job_title
ORDER BY min_hire_date;
