SELECT c.segment, COUNT(a.id) AS account_count FROM customers c INNER JOIN accounts a ON c.id = a.customer_id GROUP BY c.segment ORDER BY account_count DESC, c.segment
