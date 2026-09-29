select order_id, id from order_items intersect all select order_id, id from order_items where unit_price is not null
