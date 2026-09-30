SELECT country, CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM suppliers
GROUP BY country, CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END;
