SELECT CASE WHEN level IN ('L5', 'L2') THEN 'group_a' ELSE 'other' END AS level_group, SORT_ARRAY(COLLECT_SET(level)) AS level_list, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
FROM employees
WHERE NOT is_remote AND job_title IN ('Manager', 'Designer', 'Director')
GROUP BY level_group;
