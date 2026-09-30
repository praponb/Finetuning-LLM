SELECT dept_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values FROM departments GROUP BY dept_name ORDER BY dept_name;
