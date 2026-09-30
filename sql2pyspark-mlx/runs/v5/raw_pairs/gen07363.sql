SELECT
    i.id,
    i.last_restock,
    i.store_id,
    i.quantity,
    ss.units
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (2, 4, 5) OR i.quantity < 235
ORDER BY store_id
