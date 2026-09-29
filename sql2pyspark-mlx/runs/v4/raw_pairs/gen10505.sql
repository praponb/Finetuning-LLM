SELECT
    id,
    plan,
    customer_id,
    start_date,
    monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY start_date, id) AS running_monthly_fee
FROM subscriptions;
