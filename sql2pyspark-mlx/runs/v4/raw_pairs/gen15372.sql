SELECT c.name, c.email, o.customer_id
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE c.signup_date BETWEEN '2023-03-01' AND '2025-05-01'
ORDER BY email, customer_id
