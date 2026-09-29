SELECT
    id,
    product_id,
    last_restock,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY last_restock) AS last_restock_pct_rank
FROM inventory
WHERE store_id IN (3, 4, 6);
