SELECT products.brand, MIN(inventory.last_restock) AS first_last_restock
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
INNER JOIN suppliers ON suppliers.id = products.supplier_id
WHERE suppliers.country = 'DE'
GROUP BY products.brand
