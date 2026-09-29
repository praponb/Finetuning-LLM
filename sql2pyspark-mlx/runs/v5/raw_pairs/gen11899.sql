select id, discount, channel, customer_id from orders where discount > (select max(discount) from orders) order by customer_id desc, channel, id limit 10
