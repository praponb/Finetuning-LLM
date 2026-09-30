SELECT store_name, sqft
FROM stores
WHERE sqft = 4713 OR region IN ('East', 'West', 'Central');
