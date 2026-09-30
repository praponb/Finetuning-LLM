SELECT
    i.id,
    i.last_restock,
    i.store_id,
    i.quantity,
    ss.product_id,
    ss.store_id store_sale_store_id,
    ss.units
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id = 8
ORDER BY i.store_id ASC;
