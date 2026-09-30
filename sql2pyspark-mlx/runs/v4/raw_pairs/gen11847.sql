WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(quantity, 1) AS quantity_rounded,
        unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price > 220
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_rounded,
        line_total,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY quantity_rounded, id) AS quantity_rounded_bucket
    FROM filtered
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded DESC, id) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        line_total,
        unit_price,
        quantity_rounded
    FROM latest
) src
UNPIVOT (
    reading FOR metric IN (line_total AS LINE_TOTAL_VAL, unit_price AS UNIT_PRICE, quantity_rounded)
);
