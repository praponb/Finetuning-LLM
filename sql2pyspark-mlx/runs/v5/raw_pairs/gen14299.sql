WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE paid_at >= '2023-03-01'
),
kept_keys AS (
    -- Set difference on keys
    SELECT status
    FROM cleaned
    EXCEPT ALL
    SELECT status
    FROM payments
    WHERE amount >= 460
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.status = k.status
),
enriched AS (
    -- Attach orders attributes
    SELECT e.*, d.status AS order_status
    FROM kept_rows e
    LEFT JOIN orders d
        ON e.order_id = d.id
    WHERE d.discount >= 0.37
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.order_id,
        src.method,
        src.order_status,
        b.channel,
        b.shipping_fee
    FROM enriched src
    LEFT JOIN orders b
        ON src.order_id <=> b.id
)
SELECT
    s.order_id,
    s.method,
    s.order_status,
    s.channel,
    s.shipping_fee,
    CASE WHEN channel RLIKE '^(mobile|partner)$' THEN 'GROUP_B' WHEN channel RLIKE '^(web|store)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS channel_zone,
    (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS max_shipment_cost
FROM joined s
