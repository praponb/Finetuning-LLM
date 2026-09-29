WITH base AS (
    SELECT
        *,
        COALESCE(channel, 'unassigned') AS channel_filled,
        ROUND(shipping_fee / amount, 2) AS amount_with_shipping,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM orders
    WHERE channel IN ('partner', 'store', 'mobile')
),
kept_keys AS (
    SELECT customer_id, channel
    FROM base
    INTERSECT ALL
    SELECT customer_id, channel
    FROM orders
    WHERE amount BETWEEN 430 AND 1140
),
reconciled AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.channel = k.channel
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY shipping_fee DESC) AS shipping_fee_cume_dist,
        DENSE_RANK() OVER (PARTITION BY status, customer_id ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_dense_rank,
        RANK() OVER (ORDER BY discount DESC NULLS FIRST) AS discount_rank
    FROM reconciled
),
enriched AS (
    SELECT
        a.*,
        b.email,
        b.signup_date
    FROM with_windows a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
SELECT
    channel,
    CASE WHEN shipping_fee_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_dense_rank_tier,
    SUM(CASE WHEN channel = 'mobile' THEN shipping_fee_dense_rank ELSE 0 END) AS mobile_shipping_fee_dense_rank,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    SUM(shipping_fee) AS total_shipping_fee,
    SUM(CASE WHEN status = 'COMPLETED' THEN shipping_fee ELSE 0 END) AS completed_shipping_fee
FROM enriched
GROUP BY channel, CASE WHEN shipping_fee_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING distinct_customer_ids < 1000
ORDER BY shipping_fee_dense_rank_tier DESC NULLS FIRST
