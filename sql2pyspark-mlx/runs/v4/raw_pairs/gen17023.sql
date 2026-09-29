SELECT id, sqft, ROUND(sqft * 1.1, 2) AS sqft_scaled, LENGTH(store_name) AS store_name_length FROM stores WHERE sqft BETWEEN 12084 AND 12835 AND sqft >= 6655 AND region = 'West';
