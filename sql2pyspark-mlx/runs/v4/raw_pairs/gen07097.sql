SELECT
    i.id,
    i.quantity,
    i.store_id,
    i.last_restock,
    ss.store_id AS store_sale_store_id,
    ss.revenue,
    ADD_MONTHS(i.last_restock, 12) AS last_restock_plus_months
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY last_restock_plus_months
