select store_id
from store_sales
where revenue > 1700
union
select store_id
from store_sales
where revenue between 1400 and 4000
order by store_id;
