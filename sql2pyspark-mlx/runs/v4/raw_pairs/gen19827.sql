SELECT
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    AVG(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior,
    LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY quantity, id) AS next_quantity,
    FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
    AVG(quantity) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
    NTILE(3) OVER (ORDER BY quantity, id) AS quantity_bucket
FROM inventory
