select id, unit_price, order_id from order_items where unit_price <= (select min(unit_price) from order_items) order by order_id desc, id limit 10
