SELECT MONTH(order_date) AS order_month, COUNT(shipping_fee) AS shipping_fee_count, ROUND((100 * SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_pending
FROM orders
WHERE store_id IN (3, 6, 20)
GROUP BY MONTH(order_date)
ORDER BY order_month, pct_pending DESC;
