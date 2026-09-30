SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, SUM(CASE WHEN city = 'Singapore' THEN rating ELSE 0 END) AS singapore_rating FROM suppliers WHERE country IN ('UK', 'US')
