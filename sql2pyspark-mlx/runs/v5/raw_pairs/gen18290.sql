select s.id, s.region, s.store_name from stores s where not exists (select 1 from store_sales ss where ss.store_id = s.id and ss.product_id in (5, 7)) order by id desc;
