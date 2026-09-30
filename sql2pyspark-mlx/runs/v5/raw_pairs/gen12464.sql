SELECT
    id,
    product_id,
    last_restock,
    quantity,
    store_id,
    LEAD(quantity, 1) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS next_quantity,
    quantity - LAG(quantity, 1) OVER (PARTITION BY product_id, store_id ORDER BY last_restock, id) AS quantity_change
FROM inventory
ORDER BY store_id DESC, next_quantity DESC
