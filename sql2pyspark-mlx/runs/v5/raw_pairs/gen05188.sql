SELECT s.region, ss.units FROM stores s LEFT JOIN store_sales ss ON s.id = ss.store_id ORDER BY units DESC, region;
