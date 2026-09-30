SELECT
    ss.id,
    ss.store_id,
    ss.sale_date,
    ss.product_id
FROM store_sales ss
WHERE ss.store_id IN (1, 3, 5) AND ss.store_id IN (SELECT id FROM stores WHERE store_name LIKE 'Air%')
ORDER BY 2 DESC, 4;
