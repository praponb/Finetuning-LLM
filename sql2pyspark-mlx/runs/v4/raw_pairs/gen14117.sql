SELECT i.store_id, s.city, p.supplier_id
FROM inventory i
LEFT OUTER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
WHERE s.region <> 'West'
ORDER BY p.supplier_id ASC;
