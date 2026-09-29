SELECT id, status, order_date, discount, LAG(discount, 1) OVER (PARTITION BY status ORDER BY order_date, id) AS prev_discount FROM orders;
