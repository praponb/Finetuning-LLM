WITH base AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        CASE WHEN plan IN ('premium', 'standard') THEN 'Core' ELSE 'Other' END AS plan_group
    FROM subscriptions
    WHERE customer_id IS NULL
      AND customer_id IS NOT NULL
)
SELECT
    id,
    customer_id,
    start_date,
    plan_group,
    CUME_DIST() OVER (PARTITION BY plan, status ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_cume_dist,
    CUME_DIST() OVER (PARTITION BY plan ORDER BY monthly_fee DESC) AS monthly_fee_cume_dist_2
FROM base
