SELECT *
FROM (
    SELECT
        id,
        store_id,
        last_restock,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC, id) AS rnk
    FROM inventory
) AS r
WHERE rnk <= 3
ORDER BY rnk;
