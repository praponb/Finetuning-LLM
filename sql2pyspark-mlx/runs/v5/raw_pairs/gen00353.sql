with latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by session_id order by duration_sec, id) as row_num
    from web_events
    qualify row_num = 1
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        lower(page) as page_lower,
        coalesce(page, 'N/A') as page_clean
    from latest
    where row_num != 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by session_id order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time, id) + interval 14 days then 1 else 0 end as new_group_flag
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        session_id,
        page,
        event_time,
        page_clean,
        sum(new_group_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
    from flagged
),
with_dims as (
    -- Attach sessions attributes
    select
        src.id,
        src.session_id,
        src.page,
        src.event_time,
        src.page_clean,
        p.country,
        p.browser,
        p.device
    from sessionized src
    left join sessions p
        on src.session_id = p.id
),
flagged_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by session_id order by event_time, id) > interval '7' day then 1 else 0 end as session_start_flag
    from with_dims
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as island_id
    from flagged_2
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        page,
        event_time,
        page_clean,
        country,
        browser,
        device,
        session_start_flag,
        island_id,
        upper(page) as page_upper,
        length(page) as page_length,
        coalesce(country, 'DIRECT') as country_filled
    from sessions_web_events
    where country in ('UK', 'SG', 'US')
),
summary as (
    -- Aggregate with conditional metrics
    select
        country,
        case when session_start_flag >= 1 then 'PREMIUM' else 'STANDARD' end as session_start_flag_tier,
        session_id,
        sum(case when device = 'desktop' then session_start_flag else 0 end) as desktop_session_start_flag,
        (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as span_minutes,
        grouping(country) as gid
    from valid
    group by grouping sets ((country, case when session_start_flag >= 1 then 'PREMIUM' else 'STANDARD' end, session_id), (country, case when session_start_flag >= 1 then 'PREMIUM' else 'STANDARD' end), (country))
    having desktop_session_start_flag >= 0
)
select
    country,
    session_start_flag_tier,
    session_id,
    desktop_session_start_flag,
    span_minutes,
    gid
from summary
