SELECT s.cost, s.delivered_date, o.shipping_fee, o.store_id, o.discount FROM shipments s INNER JOIN orders o ON s.order_id = o.id
