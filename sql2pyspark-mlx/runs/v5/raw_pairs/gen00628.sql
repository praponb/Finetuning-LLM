SELECT
    id,
    country,
    supplier_name,
    city
FROM suppliers
WHERE city IS NOT NULL;
