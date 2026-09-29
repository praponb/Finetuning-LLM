SELECT shipping_fee, 'order' AS kind FROM orders WHERE customer_id IS NULL UNION SELECT cost, 'product' AS kind FROM products WHERE launch_date BETWEEN '2024-07-01' AND '2025-04-15'
