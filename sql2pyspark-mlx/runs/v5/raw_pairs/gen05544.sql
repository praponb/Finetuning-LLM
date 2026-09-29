select units
from store_sales
where store_id in (4, 5)
union all
select units
from store_sales
where product_id = 3
order by units desc;
