WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, unit_price + quantity AS line_total
    FROM order_items
    WHERE product_id = 2
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, line_total - quantity AS line_total_minus_quantity
    FROM filtered
    WHERE product_id IS NOT NULL
      AND order_id IN (12, 22, 30, 35)
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        src.unit_price,
        src.line_total_minus_quantity,
        b.status
    FROM cleaned src
    LEFT JOIN orders b
        ON src.order_id = b.id
    WHERE b.discount >= 0.17
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        product_id,
        status
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY quantity, id) = 1
)
-- Multi-level aggregation
SELECT
    order_id,
    status,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    COUNT_IF(status = 'PENDING') AS pending_events
FROM latest
GROUP BY 1, 2;
