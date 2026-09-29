SELECT
    i.id,
    i.store_id,
    i.product_id,
    s.store_name,
    s.city,
    p.supplier_id
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
INNER JOIN products p ON p.id = i.product_id
WHERE s.sqft BETWEEN 5637 AND 12064 AND YEAR(s.opened_date) >= 2025
