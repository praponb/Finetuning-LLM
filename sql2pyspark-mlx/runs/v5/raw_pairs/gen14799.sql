with base as (
    -- Keep recent, valid rows
    select
        *,
        lower(status) as status_lower,
        date_trunc('week', shipped_date) as shipped_date_period,
        coalesce(carrier, 'OTHER') as carrier_clean
    from shipments
    where order_id is not null
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_lower,
        shipped_date_period,
        carrier_clean,
        upper(status) as status_upper
    from base
    where delivered_date >= to_date('2023-06-09')
      and order_id is not null
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when shipped_date_period - lag(shipped_date_period) over (partition by order_id order by shipped_date_period asc, id asc) > interval 30 days then 1 else 0 end as new_group_flag
    from valid
),
sessions_shipments as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        carrier,
        shipped_date,
        status,
        status_lower,
        shipped_date_period,
        carrier_clean,
        status_upper,
        sum(new_group_flag) over (partition by order_id order by shipped_date_period asc, id asc rows between unbounded preceding and current row) as island_id
    from gaps
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        status,
        status_lower,
        status_upper,
        island_id,
        row_number() over (order by shipped_date_period asc, id asc) as seq_no,
        avg(island_id) over (order by shipped_date asc, id asc rows between 3 preceding and current row) as island_id_avg_3,
        last_value(carrier) ignore nulls over (partition by order_id order by shipped_date asc, id asc rows between unbounded preceding and current row) as last_known_carrier,
        avg(island_id) over (partition by order_id order by shipped_date asc, id asc rows between 2 preceding and 1 preceding) as island_id_avg_2_prior
    from sessions_shipments
    where year(shipped_date) >= 2023
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by status order by island_id_avg_2_prior desc, id asc) as dedup_rank
        from metrics
    ) x
    where dedup_rank = 1
),
valid_shipments as (
    -- Basic cleansing and defaults
    select *, cast(island_id_avg_3 as double) as island_id_avg_3_str
    from ranked
    where island_id_avg_2_prior < 3
)
-- Window metrics over several different partitions
select
    id,
    status,
    status_lower,
    island_id,
    seq_no,
    last_known_carrier,
    dedup_rank,
    avg(island_id_avg_2_prior) over (partition by status order by island_id_avg_3_str asc, id asc rows between unbounded preceding and current row) as running_avg_island_id_avg_2_prior,
    sum(island_id_avg_2_prior) over (partition by status order by island_id_avg_3_str asc, id asc rows between unbounded preceding and current row) as running_sum_island_id_avg_2_prior,
    min(island_id_avg_2_prior) over (partition by status order by island_id_avg_3_str asc, id asc rows between unbounded preceding and current row) as running_min_island_id_avg_2_prior
from valid_shipments
