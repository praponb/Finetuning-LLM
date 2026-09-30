SELECT
    id,
    product_id,
    last_restock,
    quantity,
    LAST_VALUE(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_quantity
FROM inventory
WHERE product_id = 8
ORDER BY id DESC;
