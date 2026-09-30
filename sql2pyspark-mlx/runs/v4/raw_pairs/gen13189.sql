WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST, id) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN monthly_fee >= 258 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    customer_id,
    DATE_TRUNC('WEEK', end_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS monthly_fee_per_basic,
    SUM(CASE WHEN plan = 'premium' THEN monthly_fee ELSE 0 END) AS premium_monthly_fee
FROM latest
GROUP BY CASE WHEN monthly_fee >= 258 THEN 'LARGE' ELSE 'SMALL' END, customer_id, DATE_TRUNC('WEEK', end_date)
