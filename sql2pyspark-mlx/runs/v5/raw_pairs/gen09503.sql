SELECT ss.id, ss.units, ss.product_id, ss.store_id FROM store_sales ss WHERE ss.store_id IN (SELECT id FROM stores WHERE store_name NOT LIKE 'R%')
