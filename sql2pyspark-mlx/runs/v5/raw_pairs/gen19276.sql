select orders.id, orders.order_date from orders where orders.id in (select order_id from payments where status = 'SUCCESS');
