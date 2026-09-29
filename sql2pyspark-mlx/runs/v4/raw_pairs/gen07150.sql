SELECT s.id, s.sqft, s.opened_date FROM stores s WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.store_id = s.id AND i.last_restock BETWEEN '2023-07-22' AND '2025-07-01')
