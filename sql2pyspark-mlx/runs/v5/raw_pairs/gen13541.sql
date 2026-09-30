with prev_events as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by page order by event_time, id) as prev_event_time
    from web_events
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 14 days then 1 else 0 end as is_new_burst
    from prev_events
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from gaps
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval 14 days then 1 else 0 end as new_group_flag
    from sessionized
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
joined as (
    -- Attach sessions attributes
    select
        s.id,
        s.session_id,
        s.page,
        s.event_time,
        s.duration_sec,
        s.is_new_burst,
        s.session_id_2,
        s.new_group_flag,
        p.country
    from islands s
    left join sessions p
        on s.session_id = p.id
    where p.device = 'tablet'
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        session_id,
        page,
        event_time,
        is_new_burst,
        new_group_flag,
        count(is_new_burst) over w_grp as last3_count_is_new_burst,
        min(is_new_burst) over w_grp as last3_min_is_new_burst,
        max(is_new_burst) over w_grp as last3_max_is_new_burst
    from joined
    window w_grp as (partition by page order by new_group_flag, id rows between 2 preceding and current row)
)
select
    id,
    last3_count_is_new_burst,
    last3_min_is_new_burst
from metrics
order by last3_count_is_new_burst, id desc;
