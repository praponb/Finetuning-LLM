SELECT
    id,
    status,
    amount,
    customer_id
FROM orders
WHERE store_id IN (12, 18, 32, 33) OR MONTH(order_date) IN (3, 6, 9)
