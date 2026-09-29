select payments.id, payments.order_id from payments where payments.order_id in (select id from orders where month(order_date) in (1, 3, 12));
