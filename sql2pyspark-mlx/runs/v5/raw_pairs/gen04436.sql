SELECT AVG(shipping_fee) AS avg_shipping_fee, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
FROM orders
