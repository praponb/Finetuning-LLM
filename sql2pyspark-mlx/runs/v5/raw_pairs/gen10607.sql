SELECT s.sqft, s.opened_date, i.last_restock FROM stores s LEFT JOIN inventory i ON i.store_id = s.id ORDER BY last_restock DESC;
