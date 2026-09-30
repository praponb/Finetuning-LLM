WITH with_windows AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY status, plan ORDER BY monthly_fee DESC, id ASC) AS monthly_fee_bucket,
        RANK() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank,
        SUM(monthly_fee) OVER (PARTITION BY plan, customer_id ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM subscriptions
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.start_date,
    s.end_date,
    s.monthly_fee,
    s.monthly_fee_bucket,
    s.monthly_fee_rank,
    s.cumulative_monthly_fee,
    CASE WHEN plan RLIKE '^(standard|enterprise)$' THEN 'EXTENDED' WHEN plan RLIKE '^(basic|premium)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS plan_zone,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM with_windows s
ORDER BY plan DESC NULLS FIRST, cumulative_monthly_fee ASC;
