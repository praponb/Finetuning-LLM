SELECT dept_name, COUNT(CASE WHEN dept_name = 'Sales' THEN 1 END) AS sales_count, SORT_ARRAY(COLLECT_SET(location)) AS location_list
FROM departments
WHERE budget BETWEEN 2444000 AND 3342000
GROUP BY dept_name
ORDER BY dept_name;
