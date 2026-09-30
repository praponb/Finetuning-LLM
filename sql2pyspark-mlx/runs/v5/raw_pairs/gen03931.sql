SELECT
    supplier_name,
    city,
    rating,
    TRIM(city) AS clean_city
FROM suppliers
WHERE country NOT IN ('DE', 'TH')
ORDER BY city ASC, 4 ASC;
