SELECT id, customer_id, store_id, order_date, discount, SUM(discount) OVER (PARTITION BY customer_id, store_id ORDER BY order_date, id) AS running_discount FROM orders ORDER BY discount DESC
