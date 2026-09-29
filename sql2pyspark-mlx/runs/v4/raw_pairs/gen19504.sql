WITH latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.product_launch_date,
        latest.category
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    city,
    SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE category = 'Grocery') AS grocery_rows,
    COUNT(*) AS cnt
FROM latest_match
GROUP BY city
