select
    id,
    product_id,
    last_restock,
    store_id,
    quantity,
    row_number() over (partition by product_id order by last_restock desc, id) as last_restock_row,
    row_number() over (partition by store_id order by quantity, id) as rn
from inventory;
