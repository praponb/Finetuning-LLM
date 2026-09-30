SELECT
    i.id,
    i.quantity,
    p.product_name,
    p.price,
    p.brand
FROM inventory i
LEFT OUTER JOIN products p ON p.id = i.product_id
ORDER BY brand, p.price DESC;
