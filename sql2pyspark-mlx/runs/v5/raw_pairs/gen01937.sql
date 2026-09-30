SELECT *, TRIM(region) AS clean_region, UPPER(city) AS city_upper
FROM stores
WHERE opened_date > '2025-05-01' AND sqft BETWEEN 9250 AND 9618
ORDER BY store_name ASC, city_upper ASC;
