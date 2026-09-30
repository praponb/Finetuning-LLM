select order_items.id, order_items.unit_price, order_items.quantity from order_items where order_items.product_id in (select id from products where array_contains(tags, 'eco')) order by 3 asc
