select s.opened_date, s.city, ss.units, ss.sale_date from stores s join store_sales ss on s.id = ss.store_id where ss.store_id in (2, 4, 5, 6) order by ss.sale_date desc;
