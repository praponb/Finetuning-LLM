WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        CASE WHEN amount >= 1560 THEN 'High' WHEN amount >= 420 THEN 'Medium' ELSE 'Low' END AS amount_band,
        COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE paid_at > DATE '2023-08-16'
      AND order_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.amount,
        a.status,
        a.amount_band,
        d.status AS order_status,
        d.amount AS order_amount
    FROM valid a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.status,
        a.amount_band,
        a.order_status,
        p.amount AS order_amount_2
    FROM with_dims a
    LEFT JOIN orders p
        ON a.order_id = p.id
    WHERE p.order_date >= '2024-07-01'
),
matched_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM enriched
    EXCEPT ALL
    SELECT order_id
    FROM payments
    WHERE amount <= 1220
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.order_id = k.order_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY order_status ORDER BY order_amount_2 DESC NULLS FIRST) AS order_amount_2_cume_dist,
        STDDEV_SAMP(order_amount_2) OVER (PARTITION BY order_status) AS order_status_stddev_samp_order_amount_2,
        DENSE_RANK() OVER (PARTITION BY order_status, order_id ORDER BY order_amount_2 NULLS FIRST) AS order_amount_2_dense_rank,
        AVG(order_amount_2) OVER (PARTITION BY order_id) AS order_id_avg_order_amount_2
    FROM verified
)
-- Join a dimension and aggregate
SELECT
    o.channel,
    e.status AS payment_status,
    COUNT(*) AS rows_total,
    AVG(e.order_amount_2) AS avg_order_amount_2,
    MAX(e.order_amount_2_dense_rank) AS max_order_amount_2_dense_rank,
    GROUPING_ID() AS gid
FROM with_windows e
JOIN orders o ON e.order_id = o.id
GROUP BY GROUPING SETS ((o.channel, e.status), (o.channel), ())
