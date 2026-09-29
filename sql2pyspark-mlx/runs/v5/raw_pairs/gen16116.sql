SELECT id, rating, city
FROM suppliers
WHERE supplier_name LIKE 'Eu%'
ORDER BY 2, id DESC
