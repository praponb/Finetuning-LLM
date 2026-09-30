SELECT ss.id, ss.sale_date
FROM store_sales ss
WHERE ss.sale_date >= '2024-01-09' AND ss.store_id NOT IN (SELECT id FROM stores WHERE sqft < 4493);
