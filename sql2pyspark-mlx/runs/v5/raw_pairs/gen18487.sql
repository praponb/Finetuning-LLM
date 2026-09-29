with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        initcap(trim(device)) as device_title,
        coalesce(browser, 'N/A') as browser_filled
    from sessions
    where customer_id is null
),
with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by browser order by started_at, id) as prev_started_at
    from cleaned
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 7 days then 1 else 0 end as is_new_burst
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as session_id
    from flagged
),
scored as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        browser,
        country,
        browser_filled,
        is_new_burst,
        cume_dist() over (partition by browser, country order by session_id desc) as session_id_cume_dist,
        min(is_new_burst) over w_grp as device_min_is_new_burst,
        dense_rank() over (partition by browser order by session_id desc) as session_id_dense_rank
    from sessionized
    window w_grp as (partition by device)
),
with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.browser,
        s.country,
        s.browser_filled,
        s.is_new_burst,
        s.session_id_dense_rank,
        latest.event_type,
        latest.event_time
    from scored s
    join lateral (
            select c.event_type, c.event_time
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.browser,
        s.event_time,
        x.web_event_event_time
    from with_latest s
    left join lateral (
            select c.event_time as web_event_event_time
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.event_time
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
),
with_prev_2 as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by browser order by event_time, id) as prev_event_time
    from latest_match
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 30 days then 1 else 0 end as is_new_burst
    from with_prev_2
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by browser order by event_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when prev_event_time - lag(prev_event_time) over (partition by browser order by prev_event_time, id) > interval 30 days then 1 else 0 end as new_group_flag
    from islands
),
sessionized_sessions as (
    -- Running sum of the flag = session number
    select
        id,
        browser,
        event_time,
        web_event_event_time,
        prev_event_time,
        sum(new_group_flag) over (partition by browser order by prev_event_time, id rows between unbounded preceding and current row) as island_id
    from flagged_2
)
select id, prev_event_time
from sessionized_sessions
order by prev_event_time desc;
