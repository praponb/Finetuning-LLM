SELECT s.id, s.monthly_fee, s.start_date, s.status, c.signup_date, c.name, CEIL(s.monthly_fee) AS monthly_fee_whole FROM subscriptions s FULL OUTER JOIN customers c ON s.customer_id = c.id;
