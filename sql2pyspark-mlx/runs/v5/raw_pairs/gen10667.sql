SELECT
    p.cost,
    i.store_id,
    i.last_restock,
    i.product_id
FROM products p
LEFT OUTER JOIN inventory i ON i.product_id = p.id
WHERE p.price >= 320
ORDER BY i.store_id, p.cost DESC;
