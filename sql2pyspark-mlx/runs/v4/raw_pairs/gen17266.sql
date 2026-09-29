select s.id, s.sqft, s.city, s.store_name from stores s where s.store_name like '%Town%' and not exists (select 1 from store_sales ss where ss.store_id = s.id and ss.product_id in (2, 8));
