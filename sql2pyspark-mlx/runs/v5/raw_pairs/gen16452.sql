SELECT
    i.quantity,
    p.supplier_id,
    p.price,
    s.sqft,
    LOWER(p.category) AS category_lower
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN stores s ON i.store_id = s.id
