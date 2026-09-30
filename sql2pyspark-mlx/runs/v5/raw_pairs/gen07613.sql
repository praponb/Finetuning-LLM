WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END AS quantity_level
    FROM order_items
    WHERE product_id IS NOT NULL
),
valid_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price + quantity AS line_total
    FROM valid
    WHERE product_id IN (2, 36, 37)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(line_total) OVER (PARTITION BY order_id) AS order_id_count_line_total,
        AVG(line_total) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS line_total_avg_2
    FROM valid_order_items
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.price,
        p.stock_qty
    FROM scored s
    JOIN products p
        ON s.product_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    APPROX_PERCENTILE(unit_price, 0.9) AS unit_price_p90,
    AVG(price) AS avg_price,
    ROUND(AVG(order_id_count_line_total), 2) AS avg_order_id_count_line_total,
    COUNT(*) AS cnt,
    ROUND(AVG(order_id_count_line_total), 2) AS avg_order_id_count_line_total_2
FROM with_dims
GROUP BY order_id
ORDER BY cnt DESC NULLS FIRST
