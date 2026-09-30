SELECT s.id, s.sqft
FROM stores s
WHERE s.store_name LIKE '%Old%' AND s.id IN (SELECT store_id FROM store_sales WHERE revenue BETWEEN 2700 AND 2800);
