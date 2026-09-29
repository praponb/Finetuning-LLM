with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(browser)) as browser_title,
        coalesce(browser, 'OTHER') as browser_filled
    from sessions
    where browser is not null
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.device,
        s.country,
        x.web_event_duration_sec
    from cleaned s
    left join lateral (
            select c.duration_sec as web_event_duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) x on true
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        web_event_duration_sec,
        first_value(country, true) over (partition by device order by web_event_duration_sec asc, id asc rows between unbounded preceding and current row) as first_known_country,
        last_value(web_event_duration_sec) ignore nulls over (order by web_event_duration_sec asc, id asc rows between unbounded preceding and current row) as last_known_web_event_duration_sec
    from latest_match
    where device = 'desktop'
),
latest_match_sessions as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.web_event_event_time
    from with_windows s
    join lateral (
            select c.event_time as web_event_event_time
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    web_event_duration_sec,
    last_known_web_event_duration_sec
from latest_match_sessions
