SELECT country, city, name, CONCAT_WS(',', city, segment) AS city_segment_key FROM customers ORDER BY city_segment_key;
