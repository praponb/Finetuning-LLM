SELECT
    i.store_id,
    i.quantity,
    p.category,
    s.sqft,
    s.city,
    ROUND(i.quantity * 1.07, 2) AS adjusted_quantity
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
JOIN stores s ON i.store_id = s.id
WHERE s.city != 'Singapore' AND i.quantity BETWEEN 152 AND 212
