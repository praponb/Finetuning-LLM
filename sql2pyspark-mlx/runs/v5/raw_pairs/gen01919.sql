SELECT
    id,
    store_id,
    quantity,
    AVG(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior,
    FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
    LEAD(quantity, 1) OVER (PARTITION BY store_id ORDER BY quantity, id) AS next_quantity,
    LEAD(quantity, 1) OVER (PARTITION BY store_id, product_id ORDER BY last_restock, id) AS next_quantity_2
FROM inventory
WHERE store_id = 2
