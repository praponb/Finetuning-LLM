WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.order_date,
        p.shipping_fee
    FROM payments src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY status) AS status_sum_amount,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST) AS amount_percent_rank
    FROM with_dims
),
metrics_payments AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(shipping_fee) OVER w_grp AS cumulative_shipping_fee,
        NTILE(4) OVER w_part AS amount_percent_rank_bucket
    FROM metrics
    WINDOW w_grp AS (PARTITION BY status ORDER BY status_sum_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY status ORDER BY amount_percent_rank, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount_percent_rank >= 0.83 THEN 'HIGH' ELSE 'LOW' END AS amount_percent_rank_tier,
    CASE WHEN shipping_fee >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_tier,
    MAX(paid_at) AS last_paid_at,
    COUNT(DISTINCT status) AS distinct_status,
    GROUPING(CASE WHEN amount_percent_rank >= 0.83 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM metrics_payments
GROUP BY GROUPING SETS ((CASE WHEN amount_percent_rank >= 0.83 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN shipping_fee >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN amount_percent_rank >= 0.83 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY distinct_status;
