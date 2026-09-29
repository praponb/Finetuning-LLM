select
    id,
    product_id,
    last_restock,
    quantity,
    quantity - lag(quantity, 1, 0) over (partition by product_id order by last_restock, id) quantity_change
from inventory
