SELECT job_title, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values FROM employees WHERE job_title = 'Manager' AND manager_id = 25 GROUP BY 1 ORDER BY level_values
