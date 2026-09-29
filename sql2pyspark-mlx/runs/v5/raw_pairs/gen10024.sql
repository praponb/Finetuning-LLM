select last_restock
from inventory
where quantity < 228
intersect
select delivered_date last_restock
from shipments
order by last_restock;
