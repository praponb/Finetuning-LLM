SELECT location, dept_name, MAX(budget) AS max_budget, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values FROM departments WHERE location = 'London' GROUP BY location, dept_name;
