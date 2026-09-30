WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock DESC) AS rn
    FROM inventory
)
SELECT
    id,
    store_id,
    last_restock,
    quantity
FROM ranked
WHERE rn = 1
ORDER BY 4 DESC;
