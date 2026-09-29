SELECT
    i.product_id,
    i.quantity,
    i.last_restock,
    ss.product_id AS store_sale_product_id,
    ss.sale_date
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
