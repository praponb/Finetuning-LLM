SELECT
    id,
    quantity,
    last_restock,
    product_id,
    WEEKOFYEAR(last_restock) AS last_restock_weekofyear
FROM inventory
WHERE store_id IN (2, 6)
ORDER BY 1, last_restock DESC
LIMIT 50;
