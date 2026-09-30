SELECT level, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
FROM employees
GROUP BY level
