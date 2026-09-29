SELECT
    i.product_id,
    i.last_restock,
    ss.units,
    ss.revenue,
    ss.product_id store_sale_product_id,
    FLOOR(i.quantity) quantity_whole
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY ss.units;
