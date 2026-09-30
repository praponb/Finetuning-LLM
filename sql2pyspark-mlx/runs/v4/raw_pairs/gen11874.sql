SELECT CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) segment_values, SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END) th_count, COUNT(DISTINCT age) unique_countrys FROM customers
