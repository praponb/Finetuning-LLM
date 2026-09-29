SELECT
    i.id,
    i.product_id,
    i.store_id,
    p.price
FROM inventory i
JOIN products p ON p.id = i.product_id
WHERE i.product_id IN (1, 2, 6) AND i.product_id = 3
ORDER BY i.product_id DESC, price DESC, id ASC
LIMIT 3;
