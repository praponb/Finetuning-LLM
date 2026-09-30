SELECT store_name, opened_date, sqft
FROM stores
WHERE store_name IS NULL AND store_name LIKE '%Exp%';
