select oi.id, oi.unit_price, oi.quantity, oi.order_id from order_items oi where oi.product_id not in (select id from products where launch_date > '2023-07-01') order by oi.id desc limit 5;
