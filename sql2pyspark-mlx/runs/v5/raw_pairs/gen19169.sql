select customers.id, customers.age from customers where not exists (select 1 from subscriptions where subscriptions.customer_id = customers.id)
