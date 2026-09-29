SELECT s.status, s.end_date, c.signup_date, c.credit_limit FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.country = 'DE' ORDER BY end_date;
