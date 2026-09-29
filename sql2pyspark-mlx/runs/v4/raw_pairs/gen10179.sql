select c.id, c.age from customers c where c.city = 'Singapore' and not exists (select 1 from subscriptions s where s.customer_id = c.id and s.customer_id = 5) order by id limit 50
