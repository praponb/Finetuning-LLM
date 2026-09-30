SELECT
    job_title,
    manager_id,
    MIN(hire_date) AS min_hire_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY job_title, manager_id
HAVING SUM(CASE WHEN level = 'L1' THEN salary ELSE 0 END) < 61;
