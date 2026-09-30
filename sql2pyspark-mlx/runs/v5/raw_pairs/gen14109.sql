SELECT location, COUNT(CASE WHEN location = 'London' THEN 1 END) AS london_count, SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
GROUP BY location
HAVING COUNT(CASE WHEN location = 'London' THEN 1 END) > 2;
