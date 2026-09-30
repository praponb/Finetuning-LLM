SELECT s.start_date, s.end_date, c.segment, c.credit_limit FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id
