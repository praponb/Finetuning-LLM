WITH base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        CEIL(cost) AS cost_whole,
        COALESCE(brand, 'DIRECT') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
)
SELECT
    s.*,
    top1.units,
    top1.store_sale_sale_date
FROM base s
JOIN LATERAL (
        SELECT c.units, c.sale_date AS store_sale_sale_date
        FROM store_sales c
        WHERE c.product_id = s.id
        ORDER BY c.sale_date DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
