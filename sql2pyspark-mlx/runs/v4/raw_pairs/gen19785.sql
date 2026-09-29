select s.city, count(i.quantity) as quantity_count, count(distinct i.quantity) as unique_stores from inventory i full outer join stores s on i.store_id = s.id group by s.city;
