SELECT
    id,
    product_id,
    last_restock,
    quantity,
    quantity - LAG(quantity, 2, 0) OVER (PARTITION BY product_id ORDER BY last_restock, id) quantity_change
FROM inventory
ORDER BY quantity_change DESC;
