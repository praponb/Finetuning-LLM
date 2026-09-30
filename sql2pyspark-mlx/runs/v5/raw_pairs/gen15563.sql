select store_id, min(last_restock) as first_last_restock
from inventory
group by 1
