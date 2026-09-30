SELECT s.id, s.monthly_fee, s.plan, c.segment FROM subscriptions s LEFT JOIN customers c ON s.customer_id = c.id WHERE YEAR(s.start_date) >= 2023;
