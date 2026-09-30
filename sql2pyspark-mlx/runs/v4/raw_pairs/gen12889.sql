select s.id, s.sqft, s.city, s.store_name from stores s where s.city = 'New York' and not exists (select 1 from store_sales ss where ss.store_id = s.id) order by s.id desc limit 10;
