SELECT *, COALESCE(quantity, 0) AS quantity_filled
FROM inventory
WHERE quantity >= 89
