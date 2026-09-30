SELECT i.product_id, p.product_name, p.brand
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id
ORDER BY brand, p.product_name
