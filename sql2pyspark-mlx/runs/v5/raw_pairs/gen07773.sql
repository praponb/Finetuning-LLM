SELECT
    id,
    store_id,
    quantity,
    SUM(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_quantity,
    COUNT(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_quantity,
    MAX(quantity) OVER (PARTITION BY store_id, product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_quantity,
    MAX(quantity) OVER (PARTITION BY store_id) AS store_id_max_quantity
FROM inventory
