WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        quantity,
        line_total,
        LEAST(unit_price, quantity) AS least_unit_price_quantity
    FROM filtered
    WHERE unit_price >= 380
),
valid_order_items AS (
    -- Keep recent, valid rows
    SELECT *, least_unit_price_quantity + line_total AS least_unit_price_quantity_plus_line_total
    FROM valid
    WHERE least_unit_price_quantity < 32
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY line_total NULLS FIRST) AS dedup_rank
    FROM valid_order_items
    QUALIFY dedup_rank = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, order_id
    FROM deduped
    INTERSECT ALL
    SELECT id, order_id
    FROM order_items
    WHERE quantity IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
),
filtered_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        least_unit_price_quantity,
        least_unit_price_quantity_plus_line_total,
        dedup_rank,
        ROUND(least_unit_price_quantity_plus_line_total * 0.9, 2) AS least_unit_price_quantity_plus_line_total_scaled
    FROM verified
    WHERE order_id IN (14, 15, 24)
)
-- Grouped metrics
SELECT
    CASE WHEN least_unit_price_quantity_plus_line_total_scaled >= 77 THEN 'LARGE' ELSE 'SMALL' END AS least_unit_price_quantity_plus_line_total_scaled_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(least_unit_price_quantity_plus_line_total), 2) AS avg_least_unit_price_quantity_plus_line_total,
    COUNT(DISTINCT id) AS distinct_ids_2,
    APPROX_PERCENTILE(least_unit_price_quantity_plus_line_total, ARRAY(0.25, 0.5, 0.75)) AS least_unit_price_quantity_plus_line_total_quartiles,
    ROUND(AVG(least_unit_price_quantity_plus_line_total), 2) AS avg_least_unit_price_quantity_plus_line_total_2
FROM filtered_order_items
GROUP BY CASE WHEN least_unit_price_quantity_plus_line_total_scaled >= 77 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_least_unit_price_quantity_plus_line_total_2 DESC;
