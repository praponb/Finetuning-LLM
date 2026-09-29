select s.id, s.product_id, s.units, s.revenue from store_sales s where s.units > (select avg(s2.units) from store_sales s2 where s2.product_id = s.product_id) order by s.id desc;
