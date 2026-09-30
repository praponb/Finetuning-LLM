SELECT is_remote, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
FROM employees
WHERE dept_id = 10
GROUP BY is_remote;
