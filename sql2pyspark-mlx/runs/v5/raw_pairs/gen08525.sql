select i.id, i.store_id, s.store_name from inventory i left join stores s on s.id = i.store_id where s.store_name like 'D%';
