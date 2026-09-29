SELECT id, store_name, ROUND(sqft * 1.1, 2) AS sqft_scaled FROM stores ORDER BY store_name DESC;
