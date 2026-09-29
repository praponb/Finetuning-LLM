SELECT
    i.id,
    i.product_id,
    i.quantity,
    i.store_id
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE cost BETWEEN 270 AND 550)
ORDER BY i.quantity DESC, id DESC;
