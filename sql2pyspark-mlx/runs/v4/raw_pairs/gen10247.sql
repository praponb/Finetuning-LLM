SELECT status, COUNT(monthly_fee) AS monthly_fee_count, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
FROM subscriptions
WHERE status = 'CANCELLED'
GROUP BY status;
