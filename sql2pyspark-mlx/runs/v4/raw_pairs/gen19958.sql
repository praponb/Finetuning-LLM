SELECT
    id,
    plan,
    end_date,
    monthly_fee,
    customer_id,
    LEAD(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY end_date, id) AS next_monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id) AS running_monthly_fee
FROM subscriptions
WHERE status IN ('PAUSED', 'CANCELLED')
ORDER BY end_date DESC;
