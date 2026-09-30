SELECT
    ss.id,
    ss.revenue,
    ss.product_id,
    ss.store_id
FROM store_sales ss
WHERE ss.units < 11 AND ss.product_id IN (SELECT id FROM products WHERE product_name LIKE '%Sho%')
