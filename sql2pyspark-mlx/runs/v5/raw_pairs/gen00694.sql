WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status IN ('RETURNED', 'LOST') THEN 'Priority' ELSE 'Other' END AS status_group
    FROM shipments
    WHERE delivered_date >= DATE '2023-08-27'
      AND order_id IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT order_id
    FROM filtered
    INTERSECT
    SELECT order_id
    FROM shipments
    WHERE cost < 36
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.order_id = k.order_id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.status,
        a.status_group,
        o.discount,
        o.amount,
        o.shipping_fee
    FROM kept_rows a
    JOIN orders o
        ON a.order_id = o.id
    WHERE o.channel NOT IN ('partner', 'web')
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, o.shipping_fee AS order_shipping_fee
    FROM enriched src
    JOIN orders o
        ON src.order_id = o.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(discount, 0) AS discount_filled,
        CAST(amount AS INT) AS amount_int
    FROM joined
    WHERE shipping_fee > 31
)
-- Join a dimension and aggregate
SELECT
    p.channel,
    p.status AS order_status,
    a.status AS shipment_status,
    COUNT(*) AS num_rows,
    AVG(a.amount_int) AS avg_amount_int,
    GROUPING_ID() AS grouping_level
FROM cleaned a
JOIN orders p ON a.order_id = p.id
GROUP BY GROUPING SETS ((p.channel, p.status, a.status), (p.channel, p.status), (p.channel), ())
ORDER BY avg_amount_int DESC NULLS FIRST
