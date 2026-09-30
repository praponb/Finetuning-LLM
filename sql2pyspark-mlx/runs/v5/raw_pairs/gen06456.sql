select ss.sale_date, s.store_name, s.opened_date, s.city, trim(s.store_name) as clean_store_name from store_sales ss join stores s on ss.store_id = s.id order by s.city
