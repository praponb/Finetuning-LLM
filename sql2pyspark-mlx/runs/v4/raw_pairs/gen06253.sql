SELECT ss.id, ss.units FROM store_sales ss WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE opened_date BETWEEN '2024-05-29' AND '2025-09-01') ORDER BY id;
