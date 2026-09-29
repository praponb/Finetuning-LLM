select orders.store_id, orders.order_date, shipments.order_id from orders left join shipments on shipments.order_id = orders.id order by shipments.order_id;
