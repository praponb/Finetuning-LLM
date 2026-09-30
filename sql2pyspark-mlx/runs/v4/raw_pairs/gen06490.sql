SELECT
    id,
    product_id,
    store_id,
    quantity,
    PERCENT_RANK() OVER (PARTITION BY product_id, store_id ORDER BY quantity DESC) AS quantity_pct_rank
FROM inventory
WHERE quantity >= 207;
