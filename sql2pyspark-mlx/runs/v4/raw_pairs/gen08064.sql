select i.id, i.store_id, s.sqft, s.store_name from inventory i inner join stores s on i.store_id = s.id where month(s.opened_date) in (1, 5, 7) and i.quantity <= 77
