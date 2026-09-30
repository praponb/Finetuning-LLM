SELECT
    id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    AVG(monthly_fee) OVER (PARTITION BY plan, status ORDER BY end_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
    LAG(monthly_fee, 1, 0) OVER (PARTITION BY customer_id, status ORDER BY end_date, id) AS prev_monthly_fee
FROM subscriptions;
