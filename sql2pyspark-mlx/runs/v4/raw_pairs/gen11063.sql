with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        line.qty,
        line.promo
    from carts
    lateral view explode(items) e as line
)
-- Running and moving aggregates
select
    *,
    round(qty / sum(qty) over (partition by customer_id, promo), 4) as qty_share,
    percent_rank() over (order by qty nulls last) as qty_percent_rank,
    percent_rank() over (partition by customer_id order by qty desc nulls last) as qty_percent_rank_2,
    dense_rank() over w_running as qty_dense_rank
from exploded
window w_running as (order by qty desc)
