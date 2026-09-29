SELECT
    id,
    supplier_name,
    rating,
    country
FROM suppliers
WHERE city IN ('New York', 'Chiang Mai') AND supplier_name LIKE 'Eur%'
