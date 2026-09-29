SELECT id, opened_date, city, sqft FROM stores WHERE (store_name LIKE '%Rive%' AND region = 'East') OR opened_date BETWEEN '2024-05-01' AND '2025-03-14' ORDER BY id LIMIT 1
