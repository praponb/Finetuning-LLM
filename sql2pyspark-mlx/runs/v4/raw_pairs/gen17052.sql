SELECT id, start_date, monthly_fee, status, ROUND(monthly_fee * 0.85, 2) AS monthly_fee_scaled FROM subscriptions WHERE status IN ('EXPIRED', 'ACTIVE') OR end_date < '2025-06-01' ORDER BY start_date
