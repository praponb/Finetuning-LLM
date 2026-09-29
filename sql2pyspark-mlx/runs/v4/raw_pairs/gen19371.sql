SELECT
    i.quantity,
    i.last_restock,
    i.store_id,
    ss.sale_date,
    ss.store_id AS store_sale_store_id,
    ss.product_id
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
