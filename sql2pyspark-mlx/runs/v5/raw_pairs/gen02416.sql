select c.id, c.is_active, c.segment from customers c where exists (select 1 from sessions s where s.customer_id = c.id)
