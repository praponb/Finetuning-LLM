WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY shipped_date DESC NULLS LAST, id) AS rn
        FROM shipments
    ) ranked
    WHERE rn = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.carrier,
        s.delivered_date,
        s.cost,
        s.rn,
        b.channel,
        b.status AS order_status,
        b.discount
    FROM deduped s
    LEFT JOIN orders b
        ON s.order_id = b.id
    WHERE b.customer_id IS NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY rn, id) AS dedup_rank
    FROM with_dims
    QUALIFY dedup_rank = 1
),
deduped_shipments AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount NULLS LAST, id) AS rank_in_group
    FROM latest
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(discount) OVER (PARTITION BY channel ORDER BY rank_in_group, id) prev_discount,
        LAG(cost) OVER (PARTITION BY channel ORDER BY rank_in_group, id) prev_cost,
        LEAD(cost) OVER (PARTITION BY channel ORDER BY rank_in_group, id) next_cost,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY rank_in_group, id) event_seq,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY cost DESC) cost_percent_rank
    FROM deduped_shipments
)
-- Join a dimension and aggregate
SELECT
    dim.status,
    dim.channel AS order_channel,
    COUNT(*) AS num_rows,
    AVG(e.event_seq) AS avg_event_seq,
    GROUPING_ID() AS level_id
FROM enriched e
INNER JOIN orders dim ON e.order_id = dim.id
GROUP BY GROUPING SETS ((dim.status, dim.channel), (dim.status))
ORDER BY status DESC;
