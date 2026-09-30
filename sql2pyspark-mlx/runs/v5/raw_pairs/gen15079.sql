SELECT
    id,
    product_id,
    last_restock,
    quantity,
    ROUND(quantity, 2) AS quantity_rounded,
    YEAR(last_restock) AS last_restock_year
FROM inventory
WHERE product_id = 5 AND last_restock > '2024-10-15';
