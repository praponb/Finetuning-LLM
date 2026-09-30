WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM shipments
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY cost NULLS FIRST) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.amount,
        b.shipping_fee,
        b.channel
    FROM deduped a
    INNER JOIN orders b
        ON a.order_id <=> b.id
    WHERE b.discount <= 0.28
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        shipped_date,
        status,
        amount,
        AVG(cost) OVER w_part cost_avg_2,
        ROW_NUMBER() OVER (ORDER BY shipping_fee, id) seq_no,
        NTILE(5) OVER (ORDER BY cost, id) cost_bucket,
        FIRST_VALUE(channel, TRUE) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel,
        SUM(shipping_fee) OVER w_ordered cumulative_shipping_fee
    FROM enriched
    WINDOW w_part AS (ORDER BY shipped_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        status,
        amount,
        cost_avg_2,
        cost_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY cost_bucket DESC, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
)
-- Join a dimension and aggregate
SELECT
    p.status AS order_status,
    s.status AS shipment_status,
    COUNT(*) AS rows_total,
    ROUND(SUM(s.amount), 2) AS total_amount,
    MIN(s.cost_bucket) AS min_cost_bucket
FROM latest s
INNER JOIN orders p ON s.order_id = p.id
GROUP BY p.status, s.status
