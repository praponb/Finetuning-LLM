SELECT country FROM customers WHERE age IS NOT NULL INTERSECT SELECT country FROM suppliers WHERE supplier_name LIKE '%Sup%' ORDER BY country
