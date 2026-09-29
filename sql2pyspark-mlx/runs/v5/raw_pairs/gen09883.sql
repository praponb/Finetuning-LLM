select
    last_restock,
    store_id,
    quantity,
    round(quantity * 1.1, 2) as quantity_adjusted
from inventory
where quantity = 82;
