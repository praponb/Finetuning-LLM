WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        li_item.product_id,
        li_item.qty,
        li_item.price
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, li_item
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM lines e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.created_at >= sub.start_date
       AND e.created_at < sub.end_date + INTERVAL 7 DAYS
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY product_id NULLS LAST, id, pos, subscription_id) AS latest_rank
        FROM matched
    ) t
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    plan,
    CASE WHEN product_id >= 9 THEN 'HIGH' ELSE 'LOW' END AS product_id_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
    PERCENTILE_APPROX(product_id, 0.95) AS product_id_p95,
    COUNT_IF(plan = 'basic') AS basic_events
FROM deduped
GROUP BY plan, product_id_tier, hour_window
ORDER BY plan DESC
