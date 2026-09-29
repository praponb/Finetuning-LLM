select id, order_date, discount, store_id from orders where order_date = (select max(order_date) from orders)
