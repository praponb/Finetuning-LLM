SELECT CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM suppliers WHERE country = 'SG' GROUP BY 1
