select o.id, o.order_id, o.quantity from order_items o where o.quantity > (select avg(o2.quantity) from order_items o2 where o2.order_id = o.order_id);
