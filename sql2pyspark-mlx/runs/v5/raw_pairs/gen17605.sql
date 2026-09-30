SELECT
    customer_id,
    MIN(order_date) AS first_order_date,
    SUM(CASE WHEN channel = 'mobile' THEN amount ELSE 0 END) AS mobile_amount,
    SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
FROM orders
WHERE store_id = 6 AND customer_id IN (12, 15, 30)
GROUP BY customer_id
HAVING SUM(CASE WHEN channel = 'mobile' THEN amount ELSE 0 END) >= 20
