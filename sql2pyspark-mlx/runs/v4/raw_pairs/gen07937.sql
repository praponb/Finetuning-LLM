SELECT c.id, c.is_active FROM customers c WHERE c.id IN (SELECT customer_id FROM orders WHERE store_id IS NOT NULL) ORDER BY c.id DESC;
