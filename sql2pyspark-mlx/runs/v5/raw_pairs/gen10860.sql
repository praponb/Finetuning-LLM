SELECT
    i.id,
    i.quantity,
    i.product_id,
    i.last_restock,
    ss.sale_date,
    ss.product_id AS store_sale_product_id,
    ss.store_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (2, 4, 5, 6) AND ss.revenue > 4000
