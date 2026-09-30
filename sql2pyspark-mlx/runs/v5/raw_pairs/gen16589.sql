SELECT
    amount_bucket,
    order_month,
    web_count,
    avg_shipping_fee
FROM (
    SELECT
        CASE WHEN amount >= 1660 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END AS amount_bucket,
        DATE_TRUNC('MONTH', order_date) AS order_month,
        SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web_count,
        ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee
    FROM orders
    WHERE shipping_fee <= 22
    GROUP BY CASE WHEN amount >= 1660 THEN 'high' WHEN amount >= 780 THEN 'mid' ELSE 'low' END, DATE_TRUNC('MONTH', order_date)
) AS sub
WHERE web_count >= 34
ORDER BY avg_shipping_fee DESC
