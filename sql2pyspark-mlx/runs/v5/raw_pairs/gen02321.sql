SELECT location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) location_values, SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) legal_budget FROM departments GROUP BY location
