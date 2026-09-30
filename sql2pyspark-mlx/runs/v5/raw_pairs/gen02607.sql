SELECT ss.id, ss.units, ss.revenue FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 14698 AND 14912);
