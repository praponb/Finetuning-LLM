select
    quantity,
    store_id,
    coalesce(quantity, 0) as quantity_filled,
    date_sub(last_restock, 66) as last_restock_minus_66d
from inventory
order by last_restock_minus_66d desc;
