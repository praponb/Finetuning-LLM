WITH with_last AS (
    SELECT
        s.id,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.tags,
        latest.quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    brand,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Home' THEN 1 END), 0), 2) AS stock_qty_per_home,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(category)) AS category_list,
    MIN(quantity) AS min_quantity,
    SUM(CASE WHEN category = 'Sports' THEN stock_qty ELSE 0 END) AS sports_stock_qty,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN category = 'Electronics' THEN 1 END), 0), 2) AS stock_qty_per_electronics
FROM with_last
GROUP BY brand
ORDER BY stock_qty_per_electronics;
