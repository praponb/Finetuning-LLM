SELECT id, region, sqft, CONCAT(city, ', ', region) AS city_region FROM stores WHERE opened_date BETWEEN '2025-03-11' AND '2025-09-01' AND MONTH(opened_date) IN (1, 3, 9) AND store_name IS NOT NULL
