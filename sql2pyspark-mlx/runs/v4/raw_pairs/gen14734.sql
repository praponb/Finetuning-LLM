SELECT c.age, s.browser, s.started_at FROM customers c LEFT OUTER JOIN sessions s ON s.customer_id = c.id WHERE c.signup_date BETWEEN '2023-09-01' AND '2025-07-01' ORDER BY started_at DESC;
