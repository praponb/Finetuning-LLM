SELECT
    id,
    rating,
    country,
    supplier_name
FROM suppliers
WHERE country IN ('SG', 'UK');
