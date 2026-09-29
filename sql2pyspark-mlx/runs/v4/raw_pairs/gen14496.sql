SELECT status, SUM(monthly_fee) AS total_monthly_fee, RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS monthly_fee_rank
FROM subscriptions
WHERE status = 'EXPIRED'
GROUP BY status
