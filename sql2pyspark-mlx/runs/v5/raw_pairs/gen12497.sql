SELECT is_remote, level, SORT_ARRAY(COLLECT_SET(level)) level_list
FROM employees
WHERE manager_id IN (4, 15, 21, 35) AND is_remote = TRUE
GROUP BY is_remote, level
ORDER BY level DESC, is_remote
LIMIT 5;
