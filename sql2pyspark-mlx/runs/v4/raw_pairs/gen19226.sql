with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        last_restock,
        coalesce(quantity, 0) as quantity_filled
    from inventory
    where store_id in (1, 2, 4, 6)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by product_id, store_id order by quantity_filled desc) as latest_rank
        from valid
    ) ranked
    where latest_rank = 1
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        last_restock,
        quantity_filled,
        latest_rank,
        sum(quantity_filled) over w_grp as cumulative_quantity_filled,
        ntile(10) over (partition by store_id order by quantity_filled desc, id) as quantity_filled_bucket,
        avg(quantity_filled) over w_ordered as quantity_filled_avg_3_prior,
        percent_rank() over w_hist as quantity_filled_percent_rank,
        min(quantity_filled) over (partition by product_id) as product_id_min_quantity_filled
    from latest
    window w_grp as (partition by store_id order by quantity_filled, id rows between unbounded preceding and current row), w_ordered as (partition by product_id order by quantity_filled, id rows between 3 preceding and 1 preceding), w_hist as (order by quantity_filled)
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.cumulative_quantity_filled,
    s.quantity_filled_avg_3_prior,
    (select max(x.quantity) from inventory x where x.product_id = s.product_id) as max_inventory_quantity
from with_windows s
order by product_id desc
