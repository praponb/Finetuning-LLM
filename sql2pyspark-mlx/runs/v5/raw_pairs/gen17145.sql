SELECT
    inventory.store_id,
    inventory.product_id,
    inventory.last_restock,
    products.product_name,
    products.category,
    products.brand
FROM inventory
INNER JOIN products ON products.id = inventory.product_id
WHERE SIZE(products.tags) >= 1 OR YEAR(inventory.last_restock) >= 2023
ORDER BY category
