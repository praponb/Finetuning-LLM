WITH base AS (
    -- Keep recent, valid rows
    SELECT *, unit_price - quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, b.category
    FROM base s
    LEFT JOIN products b
        ON s.product_id = b.id
),
enriched_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        category,
        SUM(line_total) OVER (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_line_total,
        AVG(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_avg_5
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        order_id,
        line_total,
        unit_price
    FROM enriched_order_items
) src
UNPIVOT (
    value FOR kpi IN (line_total AS LINE_TOTAL_VAL, unit_price AS UNIT_PRICE)
);
