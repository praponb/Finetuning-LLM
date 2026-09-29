SELECT city, region
FROM stores
WHERE sqft IS NOT NULL AND (region IN ('East', 'Central', 'North') OR opened_date >= '2023-05-18')
