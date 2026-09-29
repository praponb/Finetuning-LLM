SELECT
    brand,
    supplier_id,
    COUNT(DISTINCT price) AS unique_brands,
    COUNT(*) AS product_count
FROM products
WHERE cost < 640 AND stock_qty = 223
GROUP BY brand, supplier_id
HAVING COUNT(DISTINCT stock_qty) < 1
ORDER BY brand DESC, product_count DESC
