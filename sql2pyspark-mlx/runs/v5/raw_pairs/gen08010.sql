SELECT s.start_date, s.end_date, c.country, c.credit_limit, c.segment FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id ORDER BY c.credit_limit;
