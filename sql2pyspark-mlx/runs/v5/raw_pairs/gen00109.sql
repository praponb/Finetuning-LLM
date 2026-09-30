with scored as (
    select
        *,
        ntile(10) over (partition by store_id order by quantity desc, id asc) as quantity_bucket,
        first_value(quantity, true) over (partition by store_id order by last_restock asc, id asc rows between unbounded preceding and current row) as first_known_quantity,
        row_number() over (partition by product_id, store_id order by quantity desc, id asc) as seq_no
    from inventory
)
select
    id,
    product_id,
    quantity,
    last_restock,
    quantity_bucket,
    first_known_quantity,
    seq_no
from scored;
