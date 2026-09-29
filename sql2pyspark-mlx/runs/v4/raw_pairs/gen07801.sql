SELECT *
FROM (
    SELECT
        id,
        product_id,
        last_restock,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS rnk
    FROM inventory
) AS t
WHERE rnk = 1;
