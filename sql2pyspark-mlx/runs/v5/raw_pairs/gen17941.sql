SELECT c.id, c.email FROM customers c WHERE c.city = 'Bangkok' AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.shipping_fee > 16);
