WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        LOWER(brand) brand_lower,
        COALESCE(brand, 'OTHER') brand_clean
    FROM products
    WHERE launch_date >= DATE '2024-03-04'
      AND category = 'Fashion'
)
SELECT
    *,
    NTILE(3) OVER (PARTITION BY supplier_id ORDER BY price, id) price_bucket,
    MIN(stock_qty) OVER (PARTITION BY supplier_id) supplier_id_min_stock_qty
FROM valid;
