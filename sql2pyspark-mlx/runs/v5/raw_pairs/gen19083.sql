select oi.id, oi.order_id, oi.product_id from order_items oi where oi.unit_price <= 310 and oi.order_id in (select id from orders where customer_id is not null)
