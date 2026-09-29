SELECT level, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, SUM(CASE WHEN level = 'L4' THEN 1 ELSE 0 END) AS l4_count FROM employees GROUP BY level;
