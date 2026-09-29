SELECT c.id, c.segment, COUNT(a.id) AS account_count FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id WHERE c.signup_date < '2024-10-21' GROUP BY c.id, c.segment ORDER BY c.segment;
