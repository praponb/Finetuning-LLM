select s.id, s.sqft, s.city from stores s where exists (select 1 from store_sales ss where ss.store_id = s.id and ss.product_id = 3) order by id desc
