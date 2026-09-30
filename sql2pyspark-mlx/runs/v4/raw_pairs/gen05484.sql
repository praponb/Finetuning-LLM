SELECT s.id, s.order_id, o.customer_id, o.status, o.store_id, DAY(o.order_date) AS order_date_day FROM shipments s FULL OUTER JOIN orders o ON o.id = s.order_id ORDER BY o.store_id DESC, id DESC
