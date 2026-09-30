select s.id, s.store_name from stores s where s.sqft >= 12914 and exists (select 1 from store_sales ss where ss.store_id = s.id and ss.sale_date > '2024-09-01') order by id desc;
