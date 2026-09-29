SELECT id, store_id, last_restock
FROM (
    SELECT
        id,
        store_id,
        last_restock,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock, id) AS rnk
    FROM inventory
    WHERE product_id = 8
) AS r
WHERE rnk <= 1;
