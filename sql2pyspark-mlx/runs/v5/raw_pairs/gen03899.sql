select o.id, o.channel, o.status from orders o where o.id in (select order_id from order_items where unit_price > 370);
