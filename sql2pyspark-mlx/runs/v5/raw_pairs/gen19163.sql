SELECT
    i.product_id,
    i.last_restock,
    p.supplier_id,
    p.product_name,
    s.city
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
FULL OUTER JOIN stores s ON s.id = i.store_id
WHERE s.region IN ('West', 'East', 'Central') AND p.price BETWEEN 550 AND 570;
