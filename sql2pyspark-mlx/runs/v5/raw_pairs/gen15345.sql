WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method, status ORDER BY amount, id) AS latest_rank
    FROM payments
    QUALIFY latest_rank = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT id, order_id
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY latest_rank DESC, id) = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        p.discount,
        p.amount
    FROM ranked e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        discount,
        amount,
        RANK() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS LAST) amount_rank,
        AVG(amount) OVER (ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) amount_avg_3_prior,
        LEAD(amount, 1) OVER (PARTITION BY order_id ORDER BY amount, id) next_amount
    FROM with_dims
)
-- Join a dimension and aggregate
SELECT
    d.channel,
    d.status,
    COUNT(*) AS rows_total,
    MIN(s.amount) AS min_amount,
    ROUND(SUM(s.amount_avg_3_prior), 2) AS total_amount_avg_3_prior,
    GROUPING_ID() AS level_id
FROM metrics s
LEFT JOIN orders d ON s.order_id = d.id
GROUP BY CUBE (d.channel, d.status)
ORDER BY min_amount DESC;
