SELECT *
FROM (
    SELECT DATE_TRUNC('MONTH', order_date) AS order_month, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
    FROM orders
    GROUP BY DATE_TRUNC('MONTH', order_date)
) AS t
WHERE shipping_fee_range < 57;
