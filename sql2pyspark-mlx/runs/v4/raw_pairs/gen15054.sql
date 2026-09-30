SELECT manager_id, SORT_ARRAY(COLLECT_SET(level)) AS level_list, MIN(hire_date) AS first_hire_date FROM employees WHERE level IN ('L4', 'L2', 'L1') AND salary > 138000 GROUP BY manager_id
