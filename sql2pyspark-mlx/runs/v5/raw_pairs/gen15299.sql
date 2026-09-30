select o.id, o.order_id, o.unit_price, o.product_id from order_items o where o.unit_price > (select avg(o2.unit_price) from order_items o2 where o2.order_id = o.order_id);
