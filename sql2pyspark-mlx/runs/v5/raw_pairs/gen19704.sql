with filtered as (
    select
        id,
        quantity,
        last_restock,
        date_trunc('YEAR', last_restock) as last_restock_period
    from inventory
    where store_id = 5
)
select
    id,
    last_restock,
    ntile(5) over (order by quantity, id) as quantity_bucket,
    avg(quantity) over (order by last_restock, id rows between 6 preceding and current row) as quantity_avg_6,
    last_value(quantity) ignore nulls over (order by quantity, id rows between unbounded preceding and current row) as last_known_quantity,
    max(quantity) over (order by cast(last_restock_period as bigint) range between 2592000 preceding and current row) as quantity_max_30d,
    cume_dist() over (order by quantity desc nulls last) as quantity_cume_dist
from filtered
