SELECT ss.id, ss.units, ss.sale_date FROM store_sales ss WHERE ss.revenue BETWEEN 1200 AND 2800 AND ss.store_id IN (SELECT id FROM stores WHERE YEAR(opened_date) >= 2023);
