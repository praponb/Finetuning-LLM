select i.id, i.product_id, i.last_restock, i.quantity, s.sqft, s.store_name from inventory i join stores s on i.store_id = s.id where s.region is not null;
