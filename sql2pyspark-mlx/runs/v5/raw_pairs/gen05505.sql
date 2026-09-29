SELECT
    i.id,
    i.quantity,
    i.product_id,
    i.last_restock,
    ss.product_id AS store_sale_product_id,
    ss.sale_date,
    DATEDIFF(CURRENT_DATE(), ss.sale_date) AS days_since_sale_date
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
