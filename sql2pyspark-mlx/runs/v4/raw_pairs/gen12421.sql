SELECT is_remote, SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM employees
WHERE hire_date > '2023-03-01'
GROUP BY is_remote
HAVING SUM(CASE WHEN level = 'L3' THEN salary ELSE 0 END) >= 24
