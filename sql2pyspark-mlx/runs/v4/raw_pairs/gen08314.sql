SELECT s.sqft, s.region, ss.product_id FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id WHERE s.sqft <= 15720 AND s.opened_date BETWEEN '2023-04-01' AND '2023-08-01';
