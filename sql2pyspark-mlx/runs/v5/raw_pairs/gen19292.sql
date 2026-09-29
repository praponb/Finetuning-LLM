SELECT segment, COUNT(DISTINCT credit_limit) AS unique_countrys, SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM customers
GROUP BY 1
HAVING COUNT(DISTINCT credit_limit) < 3;
