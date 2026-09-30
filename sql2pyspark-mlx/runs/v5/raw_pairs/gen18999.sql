select i.id, i.store_id, i.last_restock, s.city, s.region from inventory i join stores s on i.store_id = s.id where s.sqft >= 8349 or i.product_id = 1;
