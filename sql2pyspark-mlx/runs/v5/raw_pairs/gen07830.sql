WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN channel IN ('web', 'store') THEN 'Core' ELSE 'Other' END AS channel_group,
        CASE WHEN discount >= 0.37 THEN 'High' WHEN discount >= 0.1 THEN 'Medium' ELSE 'Low' END AS discount_tier
    FROM orders
    WHERE channel IS DISTINCT FROM 'partner'
      AND channel IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        order_date,
        status,
        shipping_fee,
        channel_group,
        discount_tier,
        LAG(shipping_fee, 1) OVER (PARTITION BY store_id ORDER BY order_date, id) AS prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY store_id, customer_id ORDER BY discount DESC, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY shipping_fee NULLS LAST) AS shipping_fee_dense_rank
    FROM filtered
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at < s.order_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY order_date DESC, id) AS dedup_rank
    FROM eligible
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        order_date,
        shipping_fee,
        prev_shipping_fee,
        seq_no,
        shipping_fee_dense_rank,
        dedup_rank,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        MAX(shipping_fee_dense_rank) OVER (PARTITION BY status) AS status_max_shipping_fee_dense_rank,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY shipping_fee NULLS FIRST) AS shipping_fee_dense_rank_2
    FROM latest
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY shipping_fee_dense_rank, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date >= s.order_date
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN status_max_shipping_fee_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS status_max_shipping_fee_dense_rank_tier,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(event_seq, 0.9) AS event_seq_p90,
    MAX(status_max_shipping_fee_dense_rank) AS max_status_max_shipping_fee_dense_rank
FROM not_flagged
GROUP BY status_max_shipping_fee_dense_rank_tier;
