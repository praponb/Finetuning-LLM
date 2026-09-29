SELECT
    last_restock,
    store_id,
    quantity,
    DATE_SUB(last_restock, 19) AS last_restock_minus_19d,
    ROUND(quantity, 1) AS quantity_rounded
FROM inventory
WHERE product_id IN (4, 5, 6) AND MONTH(last_restock) IN (7, 8, 11) AND quantity < 81;
