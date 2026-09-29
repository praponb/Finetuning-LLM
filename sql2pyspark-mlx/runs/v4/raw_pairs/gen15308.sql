select id, channel, customer_id from orders where discount > 0.27 union all select id, channel, customer_id from orders where store_id in (10, 19, 27)
