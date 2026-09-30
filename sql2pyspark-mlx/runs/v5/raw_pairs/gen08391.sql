with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval '14' day then 1 else 0 end as new_group_flag
    from sessions
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval '30' day then 1 else 0 end as session_start_flag
    from islands
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_id_2
    from flagged
),
with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by customer_id order by started_at, id) as prev_started_at
    from sessionized
),
with_gaps_2 as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 30 days then 1 else 0 end as new_group_flag_2
    from with_prev
),
islands_sessions as (
    -- Running sum of the flag = session number
    select
        id,
        customer_id,
        device,
        started_at,
        new_group_flag,
        session_id,
        session_id_2,
        sum(new_group_flag_2) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_id_3
    from with_gaps_2
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by customer_id order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by customer_id order by started_at, id) + interval 7 days then 1 else 0 end as new_group_flag_2
    from islands_sessions
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag_2) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as session_id_4
    from flagged_2
)
-- Grouped metrics
select
    date_trunc('year', started_at) as year_start,
    session_id,
    size(collect_set(device)) as num_devices,
    avg(new_group_flag_2) filter (where device = 'desktop') as desktop_avg_new_group_flag_2,
    sum(new_group_flag) filter (where device = 'tablet') as tablet_total_new_group_flag,
    count(*) as event_count,
    round(sum(session_id) / nullif(count(case when device = 'tablet' then 1 end), 0), 2) as session_id_per_tablet,
    sort_array(collect_set(device)) as device_list,
    grouping_id() as gid
from sessions_sessions
group by rollup (date_trunc('year', started_at), session_id)
order by session_id_per_tablet desc nulls last
