SELECT
    id,
    store_id,
    quantity,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS dense_rnk
FROM inventory
WHERE last_restock BETWEEN '2023-03-01' AND '2024-01-01';
