SELECT level, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list FROM employees WHERE is_remote GROUP BY level HAVING MIN(salary) >= 112000;
