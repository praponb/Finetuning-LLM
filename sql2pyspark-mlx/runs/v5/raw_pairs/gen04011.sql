SELECT
    i.id,
    i.store_id,
    i.product_id,
    ss.store_id AS store_sale_store_id,
    ss.product_id AS store_sale_product_id,
    ss.sale_date,
    ROUND(ss.units * 0.9, 2) AS units_adjusted
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY store_sale_product_id ASC, ss.store_id DESC
