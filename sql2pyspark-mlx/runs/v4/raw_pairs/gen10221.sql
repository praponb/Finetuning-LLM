SELECT
    i.id,
    i.quantity,
    p.product_name,
    p.supplier_id,
    s.sqft
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
WHERE s.sqft = 7355
