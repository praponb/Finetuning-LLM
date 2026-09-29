SELECT
    i.id,
    i.last_restock,
    p.price,
    s.region
FROM inventory i
RIGHT OUTER JOIN products p ON p.id = i.product_id
INNER JOIN stores s ON i.store_id = s.id
WHERE LOWER(s.store_name) LIKE '%irp%' OR p.supplier_id = 39
ORDER BY price DESC, i.id
