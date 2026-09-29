SELECT s.id, s.plan, s.customer_id, s.status, c.name, c.is_active FROM subscriptions s INNER JOIN customers c ON c.id = s.customer_id WHERE c.credit_limit BETWEEN 10800 AND 12100 AND NOT c.is_active;
