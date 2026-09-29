SELECT s.id, s.cost, s.order_id, s.delivered_date, o.discount, o.customer_id FROM shipments s RIGHT JOIN orders o ON s.order_id = o.id ORDER BY customer_id DESC
