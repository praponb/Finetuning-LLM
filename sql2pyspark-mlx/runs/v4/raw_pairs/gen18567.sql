SELECT c.segment, MAX(s.monthly_fee) - MIN(s.monthly_fee) AS monthly_fee_range FROM subscriptions s LEFT JOIN customers c ON c.id = s.customer_id GROUP BY c.segment ORDER BY monthly_fee_range DESC
