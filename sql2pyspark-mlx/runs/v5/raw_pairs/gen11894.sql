SELECT c.segment, MAX(s.end_date) AS latest_end_date, MIN(s.start_date) AS first_start_date FROM subscriptions s FULL OUTER JOIN customers c ON c.id = s.customer_id GROUP BY c.segment;
