SELECT id, city, rating
FROM suppliers
WHERE supplier_name NOT LIKE '%City%'
