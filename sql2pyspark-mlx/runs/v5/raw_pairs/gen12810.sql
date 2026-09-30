SELECT
    DATE_TRUNC('MONTH', store_sales.sale_date) AS sale_month,
    store_sales.product_id,
    MAX(inventory.quantity) - MIN(inventory.quantity) AS quantity_range,
    SUM(inventory.quantity) AS total_quantity
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
GROUP BY DATE_TRUNC('MONTH', store_sales.sale_date), store_sales.product_id
ORDER BY product_id ASC;
