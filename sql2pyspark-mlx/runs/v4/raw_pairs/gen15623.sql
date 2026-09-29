SELECT i.id, i.store_id, i.quantity, i.last_restock FROM inventory i WHERE MONTH(i.last_restock) IN (2, 4, 12) AND i.store_id IN (SELECT id FROM stores WHERE YEAR(opened_date) >= 2023)
