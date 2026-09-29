select i.id, i.quantity, s.sqft, s.region, s.store_name from inventory i inner join stores s on s.id = i.store_id
