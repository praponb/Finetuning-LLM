select o.id, o.shipping_fee, o.channel, o.order_date from orders o where o.id in (select order_id from order_items where unit_price <= 170);
