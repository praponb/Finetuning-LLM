SELECT
    city,
    supplier_name,
    rating,
    TRIM(country) AS clean_country
FROM suppliers
WHERE NOT (rating > 2.4 AND city IN ('Berlin', 'Chiang Mai'))
