SELECT s.city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.channel))) AS channel_values, COUNT(o.shipping_fee) AS shipping_fee_count
FROM orders o
JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY s.city
ORDER BY city
