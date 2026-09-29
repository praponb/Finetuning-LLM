select orders.amount, orders.store_id, payments.order_id, payments.paid_at, payments.status from orders inner join payments on payments.order_id = orders.id where orders.amount >= 1080;
