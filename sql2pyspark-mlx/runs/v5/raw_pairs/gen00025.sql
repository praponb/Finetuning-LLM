SELECT c.id, c.segment, COUNT(s.id) session_count FROM customers c LEFT JOIN sessions s ON s.customer_id = c.id GROUP BY c.id, c.segment ORDER BY session_count, segment DESC
