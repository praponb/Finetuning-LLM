SELECT id, sqft, city, store_name FROM stores WHERE sqft = (SELECT MIN(sqft) FROM stores WHERE opened_date BETWEEN '2023-07-01' AND '2024-05-01') ORDER BY sqft DESC, city ASC, id ASC LIMIT 5
