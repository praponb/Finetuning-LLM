with valid as (
    -- Basic cleansing and defaults
    select
        id,
        event_type,
        page,
        event_time,
        date_trunc('WEEK', event_time) as event_time_period,
        concat_ws('-', event_type, page) as event_type_page_key
    from web_events
    where page = '/cart'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        last_value(page, true) over w_grp as last_known_page,
        row_number() over (order by event_time, id) as row_seq,
        first_value(page) ignore nulls over w_grp as first_known_page
    from valid
    window w_grp as (order by event_time_period, id rows between unbounded preceding and current row)
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time_period) over (partition by event_type order by event_time_period, id) is null
                 or event_time_period > lag(event_time_period) over (partition by event_type order by event_time_period, id) + interval '14' day then 1 else 0 end as new_group_flag
    from metrics
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select
        id,
        event_type,
        event_time,
        event_time_period,
        event_type_page_key,
        sum(new_group_flag) over (partition by event_type order by event_time_period, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
metrics_web_events as (
    -- Ranking and gap features
    select
        id,
        event_time,
        event_type_page_key,
        burst_id,
        sum(burst_id) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as running_burst_id,
        row_number() over (partition by event_type order by burst_id desc, id) as event_seq,
        avg(burst_id) over (partition by event_type) as event_type_avg_burst_id,
        ntile(10) over (order by burst_id desc, id) as burst_id_bucket
    from sessions_web_events
),
kept_keys as (
    -- Set difference on keys
    select id
    from metrics_web_events
    except
    select id
    from web_events
    where session_id in (29, 39)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics_web_events s
    join kept_keys k on s.id = k.id
)
-- Grouped metrics
select
    burst_id,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    max(event_time) as last_seen,
    round(avg(event_seq), 2) as avg_event_seq,
    approx_percentile(burst_id, 0.9) as burst_id_p90
from verified
group by burst_id
having distinct_ids_2 < 1000
order by burst_id
