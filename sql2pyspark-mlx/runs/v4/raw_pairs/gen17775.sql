SELECT
    i.id,
    i.store_id,
    i.last_restock,
    i.quantity,
    ss.store_id AS store_sale_store_id,
    ss.revenue + ss.units AS revenue_per_unit
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.last_restock >= '2025-07-01' AND i.last_restock >= '2023-03-01'
ORDER BY store_id;
