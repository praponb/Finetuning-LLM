with base as (
    select
        *,
        hour(started_at) as started_at_hour,
        length(device) as device_length
    from sessions
    where started_at > date '2023-05-22'
),
metrics as (
    select
        *,
        avg(started_at_hour) over (partition by customer_id order by started_at_hour asc, id asc rows between 2 preceding and current row) as started_at_hour_avg_2,
        sum(started_at_hour) over (partition by customer_id, device order by device_length asc, id asc rows between unbounded preceding and current row) as cumulative_started_at_hour,
        count(started_at_hour) over (partition by customer_id) as customer_id_count_started_at_hour,
        avg(started_at_hour) over (partition by device) as device_avg_started_at_hour,
        first_value(started_at_hour) ignore nulls over (order by started_at_hour asc, id asc rows between unbounded preceding and current row) as first_known_started_at_hour
    from base
),
with_gaps as (
    select *, case when lag(started_at) over (partition by country order by started_at asc, id asc) is null
                 or started_at > lag(started_at) over (partition by country order by started_at asc, id asc) + interval '30' day then 1 else 0 end as new_group_flag
    from metrics
),
sessions_sessions as (
    select *, sum(new_group_flag) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as island_id
    from with_gaps
),
eligible as (
    select s.*
    from sessions_sessions s
    where exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.event_time <= s.started_at
        )
)
select
    id,
    started_at,
    cumulative_started_at_hour,
    device_avg_started_at_hour,
    first_known_started_at_hour,
    island_id
from eligible
order by cumulative_started_at_hour asc nulls first, started_at asc
