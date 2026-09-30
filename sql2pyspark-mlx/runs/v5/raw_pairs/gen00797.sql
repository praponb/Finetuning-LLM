SELECT
    id,
    region,
    sqft,
    city,
    TRIM(city) AS clean_city
FROM stores
WHERE opened_date BETWEEN '2023-10-22' AND '2024-12-15'
