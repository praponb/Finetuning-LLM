select store_id, sum(units) as total_units, count(*) as cnt, rank() over (order by sum(units) desc) as units_rank from store_sales group by store_id order by units_rank desc;
