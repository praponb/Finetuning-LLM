SELECT
    ss.id,
    ss.revenue,
    ss.sale_date,
    ss.store_id
FROM store_sales ss
WHERE ss.product_id IN (1, 5, 8, 10) AND ss.store_id NOT IN (SELECT id FROM stores WHERE store_name IS NOT NULL);
