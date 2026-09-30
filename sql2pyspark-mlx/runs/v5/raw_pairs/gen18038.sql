SELECT
    id,
    product_id,
    store_id,
    last_restock,
    ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock DESC, id) AS last_restock_row
FROM inventory
WHERE quantity = 138;
