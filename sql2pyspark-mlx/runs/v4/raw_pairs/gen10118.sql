select oi.id, oi.quantity, oi.order_id, o.status, o.shipping_fee, o.discount from order_items oi left outer join orders o on oi.order_id = o.id where o.order_date <= '2025-05-01'
