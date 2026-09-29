SELECT customer_id, status FROM orders EXCEPT ALL SELECT customer_id, status FROM orders WHERE status IN ('SHIPPED', 'RETURNED')
