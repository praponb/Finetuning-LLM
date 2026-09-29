SELECT
    *,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
    AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee,
    LAG(monthly_fee, 1, 0) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS prev_monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share_2
FROM subscriptions
