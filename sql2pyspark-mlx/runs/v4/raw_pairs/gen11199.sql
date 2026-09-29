SELECT c.segment, MAX(s.end_date) AS last_end_date FROM subscriptions s INNER JOIN customers c ON c.id = s.customer_id GROUP BY c.segment;
