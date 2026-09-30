SELECT i.id, i.last_restock, s.opened_date, s.sqft, p.supplier_id FROM inventory i JOIN stores s ON i.store_id = s.id FULL OUTER JOIN products p ON i.product_id = p.id;
