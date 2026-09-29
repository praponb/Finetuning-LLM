SELECT
    id,
    product_id,
    quantity,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS dense_rnk
FROM inventory
ORDER BY dense_rnk DESC;
