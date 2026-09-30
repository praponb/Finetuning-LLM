SELECT ss.id, ss.product_id, ss.sale_date FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE MONTH(opened_date) IN (2, 7, 8));
