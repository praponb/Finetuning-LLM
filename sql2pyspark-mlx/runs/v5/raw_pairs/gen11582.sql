SELECT s.id, s.customer_id, c.credit_limit, c.age, c.country FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.city IN ('Singapore', 'Tokyo')
