SELECT s.region, ss.revenue FROM stores s INNER JOIN store_sales ss ON ss.store_id = s.id WHERE s.city <> 'Chiang Mai' AND ss.product_id IN (1, 4, 6, 10)
