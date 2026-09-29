SELECT channel, DATE_TRUNC('MONTH', order_date) AS order_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
FROM orders
GROUP BY channel, DATE_TRUNC('MONTH', order_date)
ORDER BY channel;
