WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(cost) AS cost_whole
    FROM products
    WHERE launch_date > TO_DATE('2023-05-26')
      AND supplier_id IS NOT NULL
)
-- Latest matching order_item per row (LATERAL ... LIMIT 1)
SELECT
    s.id,
    s.category,
    s.brand,
    s.price,
    s.stock_qty,
    latest.unit_price
FROM base s
LEFT JOIN LATERAL (
        SELECT c.unit_price
        FROM order_items c
        WHERE c.product_id = s.id
        ORDER BY c.quantity DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE
