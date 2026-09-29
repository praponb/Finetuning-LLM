SELECT
    i.quantity,
    i.product_id,
    i.last_restock,
    ss.store_id,
    ss.revenue,
    ss.sale_date
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id;
