SELECT subscriptions.id, subscriptions.customer_id, customers.segment FROM subscriptions RIGHT JOIN customers ON customers.id = subscriptions.customer_id;
