select * from (select order_id, count(*) as row_count from order_items group by order_id) as x where row_count >= 9 order by order_id desc, row_count asc limit 5
