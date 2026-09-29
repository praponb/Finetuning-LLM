SELECT customers.id, customers.name, customers.city
FROM customers
WHERE NOT EXISTS (SELECT 1 FROM subscriptions WHERE subscriptions.customer_id = customers.id)
ORDER BY customers.city
