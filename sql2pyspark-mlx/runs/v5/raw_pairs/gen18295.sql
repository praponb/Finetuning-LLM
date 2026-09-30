SELECT dept_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
WHERE location IN ('London', 'Chiang Mai', 'Singapore') AND location IN ('Singapore', 'Bangkok', 'Tokyo')
GROUP BY dept_name
ORDER BY 2 DESC, dept_name
LIMIT 50;
