SELECT city, MIN(credit_limit) AS min_credit_limit, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values
FROM customers
WHERE (is_active AND signup_date < '2024-02-01') OR city = 'Bangkok'
GROUP BY city
HAVING SUM(CASE WHEN country = 'SG' THEN credit_limit ELSE 0 END) < 72;
