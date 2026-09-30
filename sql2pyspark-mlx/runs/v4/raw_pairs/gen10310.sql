SELECT
    id,
    store_id,
    quantity,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity) AS dense_rnk
FROM inventory
WHERE store_id IN (2, 4, 5)
