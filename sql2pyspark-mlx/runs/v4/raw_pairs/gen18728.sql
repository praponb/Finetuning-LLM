SELECT channel
FROM orders
WHERE order_date BETWEEN '2023-06-01' AND '2025-08-01' AND shipping_fee > 23
