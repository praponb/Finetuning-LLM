SELECT
    id,
    plan,
    start_date,
    monthly_fee,
    ROUND(AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS monthly_fee_moving_avg_4
FROM subscriptions;
