SELECT is_remote, SORT_ARRAY(COLLECT_SET(level)) AS level_list, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list
FROM employees
GROUP BY is_remote;
