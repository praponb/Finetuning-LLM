select oi.id, oi.order_id, oi.unit_price, oi.product_id from order_items oi where oi.product_id in (select id from products where stock_qty > 338)
