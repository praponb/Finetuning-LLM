SELECT customers.id, customers.is_active, customers.city
FROM customers
WHERE customers.id IN (SELECT customer_id FROM orders)
