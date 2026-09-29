select o.id, o.amount, o.status, c.credit_limit, c.city from orders o inner join customers c on o.customer_id = c.id
