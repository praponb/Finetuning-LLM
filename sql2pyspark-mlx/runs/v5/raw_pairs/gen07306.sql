SELECT i.id, i.quantity FROM inventory i WHERE i.store_id IN (SELECT id FROM stores WHERE opened_date >= '2025-03-05');
