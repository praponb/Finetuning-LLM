SELECT
    id,
    store_id,
    ROUND(quantity, 0) quantity_rounded,
    FLOOR(quantity) quantity_whole
FROM inventory
WHERE quantity IS NOT NULL
ORDER BY quantity_rounded DESC, quantity_whole DESC;
