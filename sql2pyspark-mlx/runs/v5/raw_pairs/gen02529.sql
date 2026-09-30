with base as (
    select
        *,
        case when quantity >= 231 then 'High' when quantity >= 98 then 'Medium' else 'Low' end as quantity_band,
        round(quantity, 2) as quantity_rounded
    from inventory
    where last_restock >= to_date('2023-05-17')
),
enriched as (
    select
        *,
        first_value(quantity_rounded) ignore nulls over (order by last_restock, id rows between unbounded preceding and current row) as first_known_quantity_rounded,
        lag(quantity_rounded, 1) over (partition by store_id, product_id order by quantity, id) as prev_quantity_rounded,
        row_number() over (order by quantity, id) as event_seq,
        row_number() over w_running as event_seq_2,
        last_value(quantity_rounded) ignore nulls over w_grp as last_known_quantity_rounded
    from base
    where store_id in (1, 2, 3, 5)
    window w_running as (partition by product_id order by last_restock, id), w_grp as (partition by product_id order by quantity_rounded, id rows between unbounded preceding and current row)
),
with_windows as (
    select
        id,
        product_id,
        quantity,
        quantity_rounded,
        prev_quantity_rounded,
        last_known_quantity_rounded,
        lag(prev_quantity_rounded, 1) over (partition by product_id order by event_seq_2, id) as prev_prev_quantity_rounded,
        row_number() over (partition by store_id order by quantity_rounded, id) as row_seq
    from enriched
),
with_dims as (
    select
        src.id,
        src.product_id,
        src.quantity,
        src.prev_quantity_rounded,
        src.prev_prev_quantity_rounded,
        src.row_seq,
        b.cost,
        b.launch_date,
        b.brand
    from with_windows src
    inner join products b
        on src.product_id = b.id
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by brand order by prev_prev_quantity_rounded desc, id) as latest_rank
        from with_dims
    ) x
    where latest_rank = 1
)
select
    *,
    sum(prev_prev_quantity_rounded) over w_recent as running_sum_prev_prev_quantity_rounded,
    count(prev_prev_quantity_rounded) over w_recent as running_count_prev_prev_quantity_rounded,
    min(prev_prev_quantity_rounded) over w_recent as running_min_prev_prev_quantity_rounded,
    max(prev_prev_quantity_rounded) over w_recent as running_max_prev_prev_quantity_rounded
from deduped
window w_recent as (partition by brand order by cost, id rows between unbounded preceding and current row);
