select
    id,
    last_restock,
    quantity,
    quantity - lag(quantity, 1, 0) over (order by last_restock, id) as quantity_change
from inventory;
