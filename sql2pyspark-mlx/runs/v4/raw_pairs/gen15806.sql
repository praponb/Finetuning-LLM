SELECT SORT_ARRAY(COLLECT_SET(city)) city_list, AVG(age) avg_age
FROM customers
WHERE segment <> 'Consumer' AND (segment <> 'Consumer' OR name LIKE '%Wong%')
ORDER BY 2 DESC
