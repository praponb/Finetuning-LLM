SELECT
    id,
    plan,
    end_date,
    monthly_fee,
    LEAD(monthly_fee, 2) OVER (PARTITION BY plan ORDER BY end_date, id) AS next_monthly_fee
FROM subscriptions
WHERE end_date > '2025-06-01'
