SELECT
    is_remote,
    job_title,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
FROM employees
WHERE salary >= 113000
GROUP BY 1, 2
ORDER BY level_values DESC, is_remote ASC, job_title ASC
LIMIT 5;
