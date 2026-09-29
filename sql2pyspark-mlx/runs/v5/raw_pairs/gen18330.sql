WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 5 THEN 'Medium' ELSE 'Low' END AS quantity_band,
        quantity - unit_price AS line_total
    FROM order_items
    WHERE order_id = 38
),
filtered_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity - line_total AS quantity_minus_line_total,
        GREATEST(unit_price, quantity) AS greatest_unit_price_quantity
    FROM filtered
    WHERE line_total >= 42
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY line_total, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_unit_price,
        MIN(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY line_total, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_unit_price,
        COUNT(unit_price) OVER (PARTITION BY order_id, product_id ORDER BY line_total, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_unit_price
    FROM filtered_order_items
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM metrics
    WHERE product_id IN (25, 30, 35)
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.quantity_band,
    s.line_total,
    s.quantity_minus_line_total,
    s.greatest_unit_price_quantity,
    (SELECT MAX(x.unit_price) FROM order_items x WHERE x.order_id = s.order_id) AS max_order_item_unit_price
FROM base s
ORDER BY unit_price DESC;
