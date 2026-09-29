WITH with_windows AS (
    SELECT
        *,
        AVG(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_3,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status, customer_id), 4) AS monthly_fee_share
    FROM subscriptions
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    monthly_fee_avg_3,
    monthly_fee_share,
    CASE WHEN plan RLIKE '^(premium|basic)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS plan_zone
FROM with_windows
