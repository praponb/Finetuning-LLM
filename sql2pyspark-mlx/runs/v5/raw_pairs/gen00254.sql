WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
        AVG(cost) OVER (PARTITION BY status, order_id) AS status_avg_cost
    FROM shipments
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date ASC NULLS FIRST, id ASC) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, b.amount
    FROM deduped s
    LEFT JOIN orders b
        ON s.order_id = b.id
    WHERE b.store_id IS NOT NULL
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        shipped_date,
        status,
        cost_share,
        latest_rank,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY carrier, order_id ORDER BY latest_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY cost ASC, id ASC) AS cost_bucket
    FROM joined
)
-- Join a dimension and aggregate
SELECT
    o.channel,
    src.status AS shipment_status,
    COUNT(*) AS num_rows,
    ROUND(AVG(src.cost_share), 2) AS avg_cost_share,
    GROUPING_ID() AS level_id
FROM with_windows_shipments src
INNER JOIN orders o ON src.order_id = o.id
GROUP BY CUBE (o.channel, src.status)
