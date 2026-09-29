SELECT
    p.id,
    p.cost,
    p.brand,
    p.price
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = p.id AND i.quantity <> 113)
ORDER BY p.price, brand DESC;
