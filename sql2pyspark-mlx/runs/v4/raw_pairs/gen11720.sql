SELECT SORT_ARRAY(COLLECT_SET(level)) AS level_list FROM employees WHERE email IS NULL OR job_title IN ('Director', 'Engineer');
