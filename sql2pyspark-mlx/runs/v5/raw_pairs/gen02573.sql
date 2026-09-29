SELECT status, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE start_date <= '2024-04-01' AND status IN ('EXPIRED', 'CANCELLED')
GROUP BY status;
