WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, quantity * unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        unit_price
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price NULLS FIRST, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_unit_price,
        MAX(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_unit_price
    FROM deduped
    WHERE unit_price <= 340
),
enriched_order_items AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.order_date,
        b.amount,
        b.status
    FROM enriched e
    INNER JOIN orders b
        ON e.order_id = b.id
),
deduped_order_items AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount NULLS LAST, id) = 1
)
-- Join a dimension and aggregate
SELECT
    o.status AS order_status,
    o.channel,
    s.status AS order_item_status,
    COUNT(*) AS cnt,
    MIN(s.running_max_unit_price) AS min_running_max_unit_price,
    MIN(s.amount) AS min_amount,
    GROUPING_ID() AS gid
FROM deduped_order_items s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY GROUPING SETS ((o.status, o.channel, s.status), (o.status), ())
