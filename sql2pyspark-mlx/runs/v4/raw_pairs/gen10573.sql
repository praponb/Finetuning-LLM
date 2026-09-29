SELECT
    id,
    rating,
    country,
    supplier_name
FROM suppliers
WHERE rating >= (SELECT MAX(rating) FROM suppliers)
ORDER BY id;
