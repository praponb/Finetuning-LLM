SELECT
    ss.id,
    ss.product_id,
    ss.sale_date,
    ss.store_id
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE region != 'South')
ORDER BY id DESC
