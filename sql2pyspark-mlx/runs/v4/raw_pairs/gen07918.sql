SELECT c.id, c.segment FROM customers c WHERE c.id IN (SELECT customer_id FROM accounts) ORDER BY c.segment DESC, id DESC
