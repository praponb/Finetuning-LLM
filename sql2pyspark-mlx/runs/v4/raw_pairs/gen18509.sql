SELECT
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_month,
    ROUND((100 * SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_pending,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee
FROM orders
WHERE channel IN ('mobile', 'web', 'partner') OR discount < 0.25
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM');
