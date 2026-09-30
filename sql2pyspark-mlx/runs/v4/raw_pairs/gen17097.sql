SELECT MONTH(order_date) AS order_month, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count, ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee
FROM orders
WHERE customer_id = 2 AND (status IN ('SHIPPED', 'CANCELLED', 'PENDING') OR customer_id IN (12, 19, 37))
GROUP BY MONTH(order_date)
ORDER BY 2 DESC;
