select s.id, s.store_name, count(o.id) order_count from stores s inner join orders o on o.store_id = s.id where o.channel is null group by s.id, s.store_name order by store_name asc, id asc limit 5
