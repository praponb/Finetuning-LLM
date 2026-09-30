select
    *,
    row_number() over w_recent as seq_no,
    first_value(quantity) ignore nulls over (partition by product_id order by quantity asc, id asc rows between unbounded preceding and current row) as first_known_quantity,
    lead(quantity, 2) over w_recent as next_quantity,
    rank() over w_grp as quantity_rank
from inventory
window w_recent as (partition by store_id order by last_restock asc, id asc), w_grp as (partition by store_id order by quantity asc);
