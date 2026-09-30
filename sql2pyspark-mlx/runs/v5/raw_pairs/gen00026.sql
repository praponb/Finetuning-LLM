SELECT country
FROM suppliers
WHERE supplier_name LIKE 'Si%'
UNION
SELECT country
FROM sessions;
