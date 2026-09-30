SELECT
    i.id,
    i.last_restock,
    s.sqft,
    p.supplier_id,
    p.product_name
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
JOIN products p ON p.id = i.product_id
WHERE UPPER(TRIM(p.product_name)) = 'WIRELESS MOUSE';
