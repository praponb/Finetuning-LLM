SELECT
    s.id,
    s.monthly_fee,
    s.customer_id,
    s.start_date
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE credit_limit > 7700)
