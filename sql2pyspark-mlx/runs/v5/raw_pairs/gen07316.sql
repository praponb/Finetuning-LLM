SELECT s.id, s.shipped_date, s.status, o.shipping_fee FROM shipments s INNER JOIN orders o ON o.id = s.order_id WHERE o.customer_id IN (7, 18, 26, 31) AND MONTH(o.order_date) IN (1, 3, 12)
