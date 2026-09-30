SELECT s.sqft, s.store_name, i.quantity, i.product_id FROM stores s LEFT JOIN inventory i ON s.id = i.store_id WHERE s.region NOT IN ('East', 'South') ORDER BY sqft, s.store_name DESC
