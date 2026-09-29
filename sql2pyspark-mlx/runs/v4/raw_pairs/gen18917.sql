SELECT
    i.last_restock,
    i.product_id,
    i.store_id,
    ss.store_id AS store_sale_store_id,
    ss.sale_date,
    ADD_MONTHS(i.last_restock, 1) AS last_restock_plus_months
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.store_id IN (1, 5, 6)
