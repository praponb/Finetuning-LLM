SELECT id, quantity
FROM inventory
WHERE quantity >= (SELECT MAX(quantity) FROM inventory)
ORDER BY id DESC
