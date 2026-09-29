SELECT dept_name, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) dept_name_values, AVG(budget) avg_budget FROM departments GROUP BY dept_name HAVING AVG(budget) < 3226000;
