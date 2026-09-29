select o.id, o.store_id, o.discount, c.credit_limit, c.segment from orders o join customers c on o.customer_id = c.id
