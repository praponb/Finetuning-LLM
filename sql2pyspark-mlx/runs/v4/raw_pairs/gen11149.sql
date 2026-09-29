WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        ROUND(amount, 1) AS amount_rounded,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM orders
    WHERE order_date >= DATE '2024-02-23'
      AND store_id IS NULL
),
joined AS (
    -- Attach stores attributes
    SELECT src.*, d.sqft
    FROM valid src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < '2023-03-01'
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        amount_rounded,
        resolved_channel,
        sqft,
        FIRST_VALUE(shipping_fee) IGNORE NULLS OVER w_running AS first_known_shipping_fee,
        LAG(shipping_fee, 1) OVER (PARTITION BY status ORDER BY shipping_fee, id) AS prev_shipping_fee,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY channel, status), 4) AS sqft_share
    FROM not_flagged
    WINDOW w_running AS (PARTITION BY status ORDER BY amount_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 1530 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    APPROX_PERCENTILE(amount_rounded, ARRAY(0.25, 0.5, 0.75)) AS amount_rounded_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(sqft_share) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS sqft_share_per_mobile
FROM metrics
GROUP BY CASE WHEN amount >= 1530 THEN 'HIGH' ELSE 'LOW' END
ORDER BY sqft_share_per_mobile;
