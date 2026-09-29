SELECT customer_id, status, SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count, MIN(shipping_fee) AS min_shipping_fee FROM orders GROUP BY customer_id, status
