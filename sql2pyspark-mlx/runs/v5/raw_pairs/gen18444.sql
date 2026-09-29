SELECT
    id,
    plan,
    end_date,
    monthly_fee,
    status,
    LAG(monthly_fee, 1) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee, id) AS prev_monthly_fee,
    LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee
FROM subscriptions
