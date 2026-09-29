SELECT
    is_remote,
    CASE WHEN salary >= 164000 THEN 'high' WHEN salary >= 58000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values,
    ROUND((100 * SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l2
FROM employees
WHERE salary < 77000 AND hire_date <= '2025-06-01'
GROUP BY is_remote, CASE WHEN salary >= 164000 THEN 'high' WHEN salary >= 58000 THEN 'mid' ELSE 'low' END
HAVING pct_l2 >= 77
