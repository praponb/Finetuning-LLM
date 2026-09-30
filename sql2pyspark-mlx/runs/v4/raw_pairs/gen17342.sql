SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, SUM(credit_limit) AS total_credit_limit
FROM customers
GROUP BY city
HAVING SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) > 22;
