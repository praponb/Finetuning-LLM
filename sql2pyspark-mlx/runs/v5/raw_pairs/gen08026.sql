WITH cleaned AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        price - stock_qty AS price_minus_stock_qty,
        COALESCE(brand, 'N/A') AS brand_clean
    FROM products
    WHERE launch_date <= '2023-03-01'
      AND launch_date > TO_DATE('2023-03-18')
)
SELECT
    a.id,
    a.category,
    a.brand,
    a.price,
    a.stock_qty,
    a.launch_date,
    a.tags,
    a.supplier_id,
    p.supplier_name,
    p.rating
FROM cleaned a
INNER JOIN suppliers p
    ON a.supplier_id = p.id
WHERE p.country = 'TH';
