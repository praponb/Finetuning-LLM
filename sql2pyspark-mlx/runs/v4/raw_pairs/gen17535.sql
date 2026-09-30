with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by event_type order by event_time, id) > interval 14 days then 1 else 0 end as is_new_session
    from web_events
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
)
-- Multi-level aggregation
select
    date_trunc('month', event_time) as activity_month,
    date_trunc('quarter', event_time) as quarter_start,
    date_trunc('week', event_time) as week_start,
    count(*) as event_count,
    sort_array(collect_set(event_type)) as event_type_list,
    sum(is_new_session) as total_is_new_session,
    max(duration_sec) as max_duration_sec,
    max(event_time) as ended_at,
    count(*) as row_count
from sessionized
group by cube (date_trunc('month', event_time), date_trunc('quarter', event_time), date_trunc('week', event_time));
