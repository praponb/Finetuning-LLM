SELECT suppliers.id, suppliers.supplier_name, suppliers.country
FROM suppliers
WHERE suppliers.rating >= 2.1 AND suppliers.id IN (SELECT supplier_id FROM products WHERE launch_date BETWEEN '2024-03-01' AND '2025-04-01')
ORDER BY suppliers.country ASC;
