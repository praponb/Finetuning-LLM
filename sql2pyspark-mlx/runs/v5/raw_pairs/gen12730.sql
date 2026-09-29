SELECT supplier_name, country
FROM suppliers
WHERE city IN ('London', 'Bangkok', 'Singapore')
EXCEPT
SELECT supplier_name, country
FROM suppliers
WHERE country IN ('US', 'TH', 'JP')
