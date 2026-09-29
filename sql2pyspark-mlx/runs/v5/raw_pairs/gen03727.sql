SELECT COUNT(CASE WHEN level = 'L5' THEN 1 END) AS l5_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
FROM employees;
