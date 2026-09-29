SELECT id, opened_date, CONCAT(region, ' ', store_name) AS region_store_name
FROM stores
WHERE sqft BETWEEN 8561 AND 14699
