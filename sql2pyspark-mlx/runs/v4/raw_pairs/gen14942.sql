select o.channel, count(oi.id) as order_item_count, max(oi.unit_price) as max_unit_price from orders o inner join order_items oi on o.id = oi.order_id group by o.channel;
