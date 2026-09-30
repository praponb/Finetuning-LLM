SELECT c.id, c.signup_date
FROM customers c
WHERE c.segment = 'SMB' AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.shipping_fee > 18)
