SELECT
    id,
    sqft,
    city,
    region
FROM stores
WHERE store_name NOT LIKE 'R%' AND YEAR(opened_date) >= 2023
ORDER BY city;
