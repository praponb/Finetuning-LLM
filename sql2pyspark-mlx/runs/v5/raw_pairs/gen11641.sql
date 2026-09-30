select
    id,
    product_id,
    last_restock,
    quantity,
    store_id,
    lead(quantity, 1, 0) over (partition by product_id order by last_restock, id) as next_quantity,
    lag(quantity, 1) over (partition by store_id order by last_restock, id) as prev_quantity
from inventory;
