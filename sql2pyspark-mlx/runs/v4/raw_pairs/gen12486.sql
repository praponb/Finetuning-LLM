select s.id, s.store_id, s.units from store_sales s where s.units < (select avg(s2.units) from store_sales s2 where s2.store_id = s.store_id) order by s.store_id desc;
