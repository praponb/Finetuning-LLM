WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price line_total
    FROM order_items
    WHERE quantity = 4
),
cleaned AS (
    SELECT
        *,
        ROUND(line_total * 1.15, 2) adjusted_line_total,
        ROUND(quantity * 0.85, 2) adjusted_quantity
    FROM valid
    WHERE line_total IS NOT NULL
),
remaining_keys AS (
    SELECT order_id
    FROM cleaned
    EXCEPT
    SELECT order_id
    FROM order_items
    WHERE unit_price > 390
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
),
enriched AS (
    SELECT
        *,
        MAX(adjusted_line_total) OVER (PARTITION BY product_id) product_id_max_adjusted_line_total,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY line_total NULLS LAST) line_total_cume_dist
    FROM kept_rows
),
report AS (
    SELECT
        product_id,
        quantity,
        adjusted_line_total,
        adjusted_quantity,
        product_id_max_adjusted_line_total
    FROM enriched
    ORDER BY product_id_max_adjusted_line_total
)
SELECT
    s.product_id,
    s.quantity,
    s.adjusted_line_total,
    s.adjusted_quantity,
    s.product_id_max_adjusted_line_total,
    (SELECT SUM(o2.quantity) FROM order_items o2 WHERE o2.product_id = s.product_id) AS sum_order_item_quantity
FROM report s;
