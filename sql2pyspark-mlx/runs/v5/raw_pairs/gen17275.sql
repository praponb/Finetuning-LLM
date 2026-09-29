SELECT ss.id, ss.product_id, ss.revenue
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 7391 AND 10965)
