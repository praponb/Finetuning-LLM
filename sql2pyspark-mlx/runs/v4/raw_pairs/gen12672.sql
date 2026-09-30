SELECT i.id, i.quantity, i.last_restock, i.product_id, s.sqft, s.city, s.opened_date FROM inventory i JOIN stores s ON s.id = i.store_id WHERE s.city <> 'Singapore' ORDER BY city DESC;
