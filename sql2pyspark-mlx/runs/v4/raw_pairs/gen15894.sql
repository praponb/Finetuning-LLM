SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM (
    SELECT
        id,
        product_id,
        last_restock,
        store_id,
        RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS rn
    FROM inventory
) AS ranked
WHERE rn <= 3
ORDER BY 2 DESC;
