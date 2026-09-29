select s.id, s.opened_date, s.city from stores s where s.city not in ('London', 'Bangkok') and exists (select 1 from store_sales ss where ss.store_id = s.id and ss.store_id in (2, 4)) order by s.city
