select
    *,
    first_value(quantity, true) over w_part as first_known_quantity,
    last_value(quantity) ignore nulls over (partition by store_id order by quantity, id rows between unbounded preceding and current row) as last_known_quantity,
    lead(quantity, 1) over (partition by store_id, product_id order by last_restock, id) as next_quantity
from inventory
window w_part as (partition by product_id, store_id order by last_restock, id rows between unbounded preceding and current row)
