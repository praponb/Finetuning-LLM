SELECT
    DATE_TRUNC('MONTH', order_date) AS order_month,
    channel,
    YEAR(order_date) AS order_year,
    COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count
FROM orders
WHERE store_id IS NOT NULL AND amount > 770 AND shipping_fee >= 36
GROUP BY order_month, channel, order_year
