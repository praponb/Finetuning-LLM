SELECT
    ss.sale_date,
    ss.product_id,
    p.brand,
    s.rating
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.product_id IN (4, 5, 8, 10)
ORDER BY ss.product_id DESC
