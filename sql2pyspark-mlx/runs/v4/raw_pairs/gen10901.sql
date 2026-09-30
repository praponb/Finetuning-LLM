SELECT ss.id, ss.sale_date FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE opened_date <= '2025-04-01');
