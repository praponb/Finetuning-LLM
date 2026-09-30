select
    id,
    product_id,
    store_id,
    last_restock,
    quantity,
    sum(quantity) over (partition by product_id, store_id order by last_restock, id rows between unbounded preceding and current row) running_quantity
from inventory;
