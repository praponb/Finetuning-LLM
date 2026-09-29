SELECT
    id,
    product_id,
    last_restock,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY last_restock) last_restock_pct_rank
FROM inventory
WHERE quantity BETWEEN 136 AND 236
