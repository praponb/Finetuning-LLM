SELECT s.id, s.end_date, s.plan, c.country FROM subscriptions s LEFT OUTER JOIN customers c ON s.customer_id = c.id;
