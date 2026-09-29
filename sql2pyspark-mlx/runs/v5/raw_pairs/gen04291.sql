with deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec
    from web_events
    qualify row_number() over (partition by session_id order by event_time desc nulls last, id asc) = 1
),
prev_events as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by event_type order by event_time asc, id asc) as prev_event_time
    from deduped
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval '14' day then 1 else 0 end as new_group_flag
    from prev_events
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        sum(new_group_flag) over (partition by event_type order by event_time asc, id asc rows between unbounded preceding and current row) as island_id
    from flagged
),
remaining_keys as (
    -- Set intersection on keys
    select session_id, id
    from sessions_web_events
    intersect all
    select session_id, id
    from web_events
    where session_id in (4, 6, 26, 28)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from sessions_web_events s
    join remaining_keys k on s.session_id = k.session_id and s.id = k.id
),
joined as (
    -- Attach sessions attributes
    select
        s.*,
        p.country,
        p.device,
        p.browser
    from reconciled s
    left join sessions p
        on s.session_id = p.id
    where p.country = 'TH'
)
-- Aggregate with conditional metrics
select
    date_trunc('week', event_time) as week_start,
    case when island_id >= 8 then 'LARGE' else 'SMALL' end as island_id_tier,
    sort_array(collect_set(event_type)) as event_type_list,
    count(*) as row_count,
    round(sum(island_id) / nullif(count(case when event_type = 'click' then 1 end), 0), 2) as island_id_per_click
from joined
group by grouping sets ((date_trunc('week', event_time), case when island_id >= 8 then 'LARGE' else 'SMALL' end), (date_trunc('week', event_time)))
order by row_count desc
