SELECT
    id,
    plan,
    status,
    MIN(monthly_fee) OVER w_part AS running_min_monthly_fee,
    AVG(monthly_fee) OVER w_part AS running_avg_monthly_fee,
    MAX(monthly_fee) OVER w_part AS running_max_monthly_fee,
    COUNT(monthly_fee) OVER w_part AS running_count_monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, status), 4) AS monthly_fee_share
FROM subscriptions
WINDOW w_part AS (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
