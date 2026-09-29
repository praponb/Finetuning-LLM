select store_id, count(*) as cnt from store_sales group by store_id order by store_id desc
