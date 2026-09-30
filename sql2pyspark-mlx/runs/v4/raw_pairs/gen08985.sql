SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee,
    LAG(monthly_fee, 2) OVER (ORDER BY monthly_fee, id) prev_monthly_fee
FROM subscriptions;
