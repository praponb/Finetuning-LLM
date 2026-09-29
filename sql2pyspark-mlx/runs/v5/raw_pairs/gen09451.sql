SELECT *, LENGTH(channel) AS channel_length, DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date FROM orders WHERE status != 'COMPLETED' AND shipping_fee <= 13 ORDER BY order_date DESC;
