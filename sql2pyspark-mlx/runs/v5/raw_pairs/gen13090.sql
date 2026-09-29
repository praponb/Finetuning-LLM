with base as (
    -- Basic cleansing and defaults
    select
        *,
        date_trunc('WEEK', last_restock) as last_restock_period,
        floor(quantity) as quantity_whole
    from inventory
    where year(last_restock) = 2025
),
prev_events as (
    -- Previous event time per key
    select *, lag(last_restock_period) over (partition by store_id order by last_restock_period, id) as prev_last_restock_period
    from base
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_last_restock_period is null or last_restock_period > prev_last_restock_period + interval 30 days then 1 else 0 end as is_new_session
    from prev_events
),
sessions_inventory as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by store_id order by last_restock_period, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by product_id order by prev_last_restock_period, id) as seq_no,
        ntile(5) over w_grp as quantity_bucket,
        avg(quantity) over (partition by product_id, store_id order by last_restock_period, id rows between 3 preceding and current row) as quantity_avg_3,
        ntile(10) over (partition by store_id order by session_no desc, id) as session_no_bucket
    from sessions_inventory
    where store_id in (2, 3, 4, 5)
    window w_grp as (partition by store_id, product_id order by quantity desc, id)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        last_restock,
        prev_last_restock_period,
        session_no,
        seq_no,
        session_no_bucket,
        sum(quantity) over (partition by product_id order by quantity_avg_3, id rows between unbounded preceding and current row) as running_quantity,
        lead(session_no_bucket, 1) over (partition by store_id order by quantity, id) as next_session_no_bucket,
        max(quantity_whole) over (partition by product_id) as product_id_max_quantity_whole,
        sum(quantity) over (partition by store_id order by seq_no, id rows between unbounded preceding and current row) as running_quantity_2
    from with_windows
),
prev_events_2 as (
    -- Previous event time per key
    select *, lag(prev_last_restock_period) over (partition by store_id order by prev_last_restock_period, id) as prev_prev_last_restock_period
    from scored
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_prev_last_restock_period is null or prev_last_restock_period > prev_prev_last_restock_period + interval '7' day then 1 else 0 end as is_new_session
    from prev_events_2
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        store_id,
        last_restock,
        prev_last_restock_period,
        product_id_max_quantity_whole,
        running_quantity_2,
        sum(is_new_session) over (partition by store_id order by prev_last_restock_period, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        last_restock,
        prev_last_restock_period,
        running_quantity_2,
        burst_id,
        running_quantity_2 - product_id_max_quantity_whole as running_quantity_2_minus_product_id_max_quantity_whole
    from sessionized
    where last_restock >= date '2023-09-15'
)
-- Grouped metrics
select
    burst_id,
    date_trunc('week', last_restock) as week_start,
    round(avg(running_quantity_2), 2) as avg_running_quantity_2,
    approx_percentile(burst_id, array(0.25, 0.5, 0.75)) as burst_id_quartiles,
    max(prev_last_restock_period) as last_seen,
    min(prev_last_restock_period) as first_prev_last_restock_period,
    count(distinct id) as distinct_ids
from filtered
group by grouping sets ((burst_id, date_trunc('week', last_restock)), (burst_id), ())
having distinct_ids > 1;
