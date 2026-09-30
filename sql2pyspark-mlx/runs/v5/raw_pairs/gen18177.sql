SELECT
    id,
    status,
    end_date,
    monthly_fee,
    LEAD(monthly_fee, 1) OVER (PARTITION BY status ORDER BY end_date, id) AS next_monthly_fee
FROM subscriptions
WHERE monthly_fee BETWEEN 70 AND 80
ORDER BY status
