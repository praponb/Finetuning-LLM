SELECT QUARTER(hire_date) AS hire_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, MAX(hire_date) AS latest_hire_date FROM employees GROUP BY 1 ORDER BY hire_quarter DESC;
