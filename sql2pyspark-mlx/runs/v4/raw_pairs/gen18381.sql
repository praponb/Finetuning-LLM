SELECT o.status, o.shipping_fee, c.signup_date, c.city FROM orders o INNER JOIN customers c ON o.customer_id = c.id WHERE c.name NOT LIKE '%bob%' AND c.signup_date < '2025-08-06' ORDER BY status;
