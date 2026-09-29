select o.id, o.channel, o.amount, c.email, c.is_active from orders o join customers c on o.customer_id = c.id order by o.channel
