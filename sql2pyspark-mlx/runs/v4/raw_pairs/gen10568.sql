SELECT c.id, c.segment FROM customers c WHERE c.credit_limit BETWEEN 13200 AND 16100 AND EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id);
