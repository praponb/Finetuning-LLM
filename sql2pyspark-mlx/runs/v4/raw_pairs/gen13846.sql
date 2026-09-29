SELECT s.id, s.monthly_fee, c.age, LENGTH(s.plan) AS plan_length FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.credit_limit > 11900
