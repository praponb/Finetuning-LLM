select s.id, s.city from stores s where s.id in (select store_id from inventory) order by s.id;
