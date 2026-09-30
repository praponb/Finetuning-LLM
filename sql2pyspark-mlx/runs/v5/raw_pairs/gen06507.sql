with valid as (
    select
        id,
        store_id,
        quantity,
        last_restock,
        dayofweek(last_restock) as last_restock_dayofweek,
        cast(quantity as double) as quantity_str
    from inventory
    where last_restock > to_date('2023-06-02')
),
with_dims as (
    select
        e.id,
        e.store_id,
        e.quantity,
        e.last_restock,
        e.last_restock_dayofweek,
        e.quantity_str,
        s.region
    from valid e
    join stores s
        on e.store_id = s.id
    where s.opened_date < '2023-10-01'
),
metrics as (
    select
        *,
        lead(quantity) over w_grp as next_quantity,
        lag(quantity) over w_grp as prev_quantity,
        lead(quantity_str) over w_grp as next_quantity_str,
        lag(quantity_str) over w_grp as prev_quantity_str,
        row_number() over w_grp as seq_no
    from with_dims
    window w_grp as (partition by store_id order by last_restock_dayofweek, id)
),
scored as (
    select
        *,
        lead(quantity) over (partition by region order by last_restock_dayofweek, id) as next_quantity_2,
        lag(prev_quantity_str) over (partition by region order by last_restock_dayofweek, id) as prev_prev_quantity_str,
        lead(prev_quantity_str) over (partition by region order by last_restock_dayofweek, id) as next_prev_quantity_str,
        row_number() over (partition by region order by last_restock_dayofweek, id) as event_seq,
        ntile(10) over (order by prev_quantity desc, id) as prev_quantity_bucket
    from metrics
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by region, store_id order by quantity nulls last, id) as rn
        from scored
    ) t
    where rn = 1
),
cleaned as (
    select
        id,
        last_restock,
        last_restock_dayofweek,
        region,
        next_quantity,
        prev_quantity,
        seq_no,
        next_quantity_2,
        next_prev_quantity_str,
        event_seq,
        prev_quantity_bucket,
        ceil(quantity_str) as quantity_str_whole,
        coalesce(region, 'DIRECT') as resolved_region
    from deduped
    where region is not null
      and next_quantity_2 >= 158
),
deduped_inventory as (
    select *, row_number() over (partition by region order by last_restock desc, id) as rn
    from cleaned
    qualify rn = 1
)
select
    id,
    last_restock_dayofweek,
    region,
    next_quantity,
    prev_quantity,
    seq_no,
    next_quantity_2,
    event_seq,
    quantity_str_whole,
    rn,
    last_value(last_restock_dayofweek) ignore nulls over w_grp as last_known_last_restock_dayofweek,
    ntile(4) over (partition by region order by prev_quantity, id) as prev_quantity_bucket_2,
    last_value(seq_no) ignore nulls over (partition by region order by quantity_str_whole, id rows between unbounded preceding and current row) as last_known_seq_no,
    lag(event_seq, 1) over (partition by region order by next_quantity, id) as prev_event_seq,
    sum(event_seq) over w_part as running_event_seq
from deduped_inventory
window w_grp as (partition by region order by prev_quantity_bucket, id rows between unbounded preceding and current row), w_part as (partition by region order by last_restock_dayofweek, id rows between unbounded preceding and current row);
