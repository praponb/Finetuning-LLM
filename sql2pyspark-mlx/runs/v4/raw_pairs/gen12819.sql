SELECT
    store_id,
    product_id,
    MAX(quantity) max_quantity,
    COUNT(quantity) quantity_count,
    SUM(quantity) total_quantity
FROM inventory
GROUP BY store_id, product_id;
