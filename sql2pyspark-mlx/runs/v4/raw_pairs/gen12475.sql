SELECT s.id, s.monthly_fee, s.end_date, c.country FROM subscriptions s JOIN customers c ON s.customer_id = c.id
