select id, store_id
from store_sales
intersect all
select id, store_id
from store_sales
where month(sale_date) in (1, 3, 9)
