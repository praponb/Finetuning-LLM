SELECT
    is_remote,
    job_title,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
    MIN(salary) AS min_salary
FROM employees
WHERE is_remote AND salary BETWEEN 83000 AND 123000
GROUP BY is_remote, job_title
