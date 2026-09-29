SELECT ss.id, ss.revenue FROM store_sales ss WHERE ss.revenue >= 2100 AND ss.store_id NOT IN (SELECT id FROM stores WHERE MONTH(opened_date) IN (5, 7, 12))
