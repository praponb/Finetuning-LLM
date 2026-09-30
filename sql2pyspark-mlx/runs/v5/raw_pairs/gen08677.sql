select o.status, o.shipping_fee, oi.unit_price from orders o left join order_items oi on o.id = oi.order_id where oi.unit_price < 290;
