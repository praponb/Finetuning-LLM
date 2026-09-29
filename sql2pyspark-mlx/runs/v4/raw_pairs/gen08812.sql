select
    *,
    avg(quantity) over (partition by store_id order by last_restock, id rows between 2 preceding and current row) as quantity_avg_2,
    percent_rank() over w_ordered as quantity_percent_rank,
    lead(quantity, 1, 0) over (partition by store_id order by quantity, id) as next_quantity
from inventory
window w_ordered as (partition by store_id order by quantity desc);
