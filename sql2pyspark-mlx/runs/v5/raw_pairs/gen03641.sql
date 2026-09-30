select o.id, o.channel from orders o where o.id in (select order_id from order_items where order_id = 8)
