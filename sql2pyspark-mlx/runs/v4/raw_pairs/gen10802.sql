SELECT
    inventory.id,
    inventory.quantity,
    products.cost,
    products.stock_qty
FROM inventory
LEFT OUTER JOIN products ON inventory.product_id = products.id
WHERE inventory.last_restock <= '2024-11-01' AND inventory.product_id = 10
ORDER BY 2;
