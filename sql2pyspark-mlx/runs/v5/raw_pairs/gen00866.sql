SELECT
    job_title,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary,
    COUNT(*) AS cnt
FROM employees
GROUP BY job_title
ORDER BY job_title;
