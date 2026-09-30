SELECT customers.id, customers.name FROM customers WHERE customers.id IN (SELECT customer_id FROM sessions WHERE customer_id IN (23, 28))
