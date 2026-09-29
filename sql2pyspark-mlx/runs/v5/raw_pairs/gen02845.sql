SELECT manager_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values FROM employees GROUP BY 1 ORDER BY manager_id DESC LIMIT 5;
