SELECT
    i.store_id,
    i.quantity,
    ss.product_id,
    ss.units,
    ss.store_id AS store_sale_store_id,
    DATE_TRUNC('month', ss.sale_date) AS sale_date_period
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY ss.units DESC
