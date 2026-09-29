SELECT
    id,
    store_id,
    quantity,
    product_id,
    ADD_MONTHS(last_restock, 6) AS last_restock_plus_months
FROM inventory
WHERE YEAR(last_restock) = 2025 AND MONTH(last_restock) IN (2, 5, 10);
