select o.status, o.discount, o.customer_id, c.segment, c.city from orders o join customers c on o.customer_id = c.id where o.amount between 410 and 500 order by customer_id desc
