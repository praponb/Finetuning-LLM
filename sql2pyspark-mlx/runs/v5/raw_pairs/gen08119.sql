WITH base AS (
    SELECT *, CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM order_items
    WHERE order_id IN (1, 2, 15, 39)
),
filtered AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM base
    WHERE product_id IS NOT NULL
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC NULLS FIRST, id) = 1
),
deduped_order_items AS (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY unit_price) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
)
SELECT
    p.brand,
    COUNT(*) AS cnt,
    SUM(a.quantity_filled) AS total_quantity_filled,
    GROUPING_ID() AS level_id
FROM deduped_order_items a
LEFT JOIN products p ON a.product_id = p.id
GROUP BY ROLLUP (p.brand);
