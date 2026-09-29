select
    id,
    store_id,
    last_restock,
    row_number() over (partition by store_id order by last_restock, id) row_num
from inventory;
