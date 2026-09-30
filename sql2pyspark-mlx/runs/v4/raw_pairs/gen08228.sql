SELECT
    s.id,
    s.monthly_fee,
    s.start_date,
    s.status
FROM subscriptions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE city IN ('London', 'Bangkok', 'Chiang Mai'))
ORDER BY s.start_date DESC
