select store_id, min(last_restock) min_last_restock
from inventory
where store_id = 3
group by store_id
order by store_id
limit 50;
