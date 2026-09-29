SELECT o.id, o.shipping_fee FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE email LIKE '%@gmail.com') ORDER BY id DESC LIMIT 1
