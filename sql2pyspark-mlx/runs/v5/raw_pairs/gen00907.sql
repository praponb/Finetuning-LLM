WITH latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        s.rating,
        latest.category,
        latest.stock_qty
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.category, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    category,
    stock_qty
FROM latest_match
ORDER BY country NULLS LAST;
