SELECT
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_month,
    ROUND(AVG(discount), 2) AS avg_discount,
    MIN(order_date) AS min_order_date,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM orders
WHERE shipping_fee <= 29 OR status = 'COMPLETED'
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
HAVING COUNT(*) < 5
