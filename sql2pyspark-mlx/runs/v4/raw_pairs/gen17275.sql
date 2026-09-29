SELECT dept_name, location, SORT_ARRAY(COLLECT_SET(location)) AS location_list, SUM(budget) AS total_budget FROM departments GROUP BY dept_name, location
