select last_restock
from inventory
where quantity = 195
union all
select last_restock
from inventory
where product_id in (7, 8);
