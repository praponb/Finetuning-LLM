with filtered as (
    -- Keep recent, valid rows
    select
        *,
        substring(browser, 1, 4) as browser_prefix,
        lower(browser) as browser_lower
    from sessions
    where started_at > timestamp '2023-04-27 14:00:00'
      and device = 'desktop'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
        )
),
prev_events as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by customer_id order by started_at, id) as prev_started_at
    from kept
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '30' day then 1 else 0 end as is_new_session
    from prev_events
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as island_id
    from gaps
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by device order by started_at desc) as dedup_rank
    from sessions_sessions
    qualify dedup_rank = 1
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.web_event_event_type
    from latest s
    left join lateral (
            select c.event_type as web_event_event_type
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        device,
        browser,
        country,
        browser_prefix,
        browser_lower,
        is_new_session,
        island_id,
        dedup_rank,
        case when device = 'tablet' then 1 else 0 end as is_tablet
    from latest_match
    where country is not distinct from 'US'
      and customer_id is not null
)
select
    id,
    device,
    browser,
    country,
    browser_prefix,
    is_new_session,
    dedup_rank,
    is_tablet,
    case when device rlike '^(tablet|mobile)$' then 'OTHER_ZONE' when device rlike '^(desktop)$' then 'EXTENDED' else 'GROUP_B' end as device_zone
from cleaned
order by device desc;
