SELECT i.id, i.quantity
FROM inventory i
WHERE i.store_id NOT IN (SELECT id FROM stores WHERE opened_date >= '2023-04-02');
