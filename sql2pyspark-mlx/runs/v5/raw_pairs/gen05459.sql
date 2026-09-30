SELECT customers.id, customers.name FROM customers WHERE EXISTS (SELECT 1 FROM subscriptions WHERE subscriptions.customer_id = customers.id);
