WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price > 150
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, order_id
    FROM valid
    EXCEPT DISTINCT
    SELECT id, order_id
    FROM order_items
    WHERE order_id IN (1, 16, 24, 28)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.order_id = k.order_id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, order_id
    FROM verified
    EXCEPT ALL
    SELECT product_id, order_id
    FROM order_items
    WHERE order_id = 35
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        order_id,
        product_id,
        unit_price,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY quantity_filled DESC) AS quantity_filled_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC) AS quantity_percent_rank,
        PERCENT_RANK() OVER w_grp AS unit_price_percent_rank
    FROM kept_rows
    WINDOW w_grp AS (ORDER BY unit_price DESC NULLS FIRST)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, unit_price_percent_rank - quantity_percent_rank AS unit_price_percent_rank_minus_quantity_percent_rank
    FROM scored
    WHERE unit_price_percent_rank BETWEEN 0.61 AND 0.73
)
-- Join a dimension and aggregate
SELECT
    p.category,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.quantity_filled_percent_rank), 2) AS total_quantity_filled_percent_rank,
    GROUPING_ID() AS grouping_level
FROM cleaned a
LEFT JOIN products p ON a.product_id = p.id
GROUP BY ROLLUP (p.category)
ORDER BY category DESC NULLS LAST
