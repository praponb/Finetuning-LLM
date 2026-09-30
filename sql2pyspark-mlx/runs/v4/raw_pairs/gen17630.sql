WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN monthly_fee >= 198 THEN 'High' WHEN monthly_fee >= 85 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND customer_id IS NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM base s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.end_date >= sub.start_date
       AND s.end_date <= DATE_ADD(sub.end_date, 10)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY subscription_id, id, subscription_id) AS rn
        FROM with_window
    ) t
    WHERE rn = 1
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN subscription_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS subscription_id_tier,
        ROUND(SUM(subscription_id) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS subscription_id_per_enterprise,
        COUNT(*) FILTER (WHERE status = 'ACTIVE') AS active_rows,
        COUNT(*) AS num_rows,
        SUM(CASE WHEN status = 'ACTIVE' THEN subscription_monthly_fee ELSE 0 END) AS active_subscription_monthly_fee
    FROM latest
    GROUP BY subscription_id_tier
)
SELECT
    subscription_id_tier,
    subscription_id_per_enterprise,
    active_rows,
    num_rows,
    active_subscription_monthly_fee
FROM daily
ORDER BY active_subscription_monthly_fee DESC NULLS FIRST;
