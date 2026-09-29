WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        price - stock_qty AS price_minus_stock_qty
    FROM products
    WHERE launch_date >= TO_DATE('2023-08-10')
)
-- Latest matching store_sale per row (LATERAL ... LIMIT 1)
SELECT
    s.id,
    s.category,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    latest.store_sale_units
FROM cleaned s
LEFT JOIN LATERAL (
        SELECT c.units AS store_sale_units
        FROM store_sales c
        WHERE c.product_id = s.id
          AND c.sale_date <= s.launch_date
        ORDER BY c.sale_date DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
