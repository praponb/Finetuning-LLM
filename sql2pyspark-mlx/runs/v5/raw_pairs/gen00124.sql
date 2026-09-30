select i.quantity, i.last_restock, s.city, s.opened_date from inventory i left join stores s on s.id = i.store_id where s.store_name like 'Air%' and s.sqft != 5961
