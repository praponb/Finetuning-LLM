SELECT SORT_ARRAY(COLLECT_SET(location)) AS location_list, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values, COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count
FROM departments
WHERE budget IS NOT NULL
ORDER BY location_values, marketing_count
