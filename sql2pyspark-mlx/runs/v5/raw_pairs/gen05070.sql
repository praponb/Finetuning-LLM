WITH product_summary AS (
    SELECT product_id, COUNT(*) store_sale_count, MIN(sale_date) first_sale_date
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.supplier_id,
    p.brand,
    ps.store_sale_count,
    ps.first_sale_date
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.store_sale_count > 6
ORDER BY store_sale_count
