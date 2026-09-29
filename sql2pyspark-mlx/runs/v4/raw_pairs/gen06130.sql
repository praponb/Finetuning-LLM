SELECT level, SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM employees
GROUP BY 1
ORDER BY level
