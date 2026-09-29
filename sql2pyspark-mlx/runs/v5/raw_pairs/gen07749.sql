with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        device,
        browser,
        concat(device, '/', country) as device_country
    from sessions
    where started_at >= date '2023-09-17'
      and browser is not null
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.browser,
        s.device_country,
        x.duration_sec,
        x.web_event_page
    from valid s
    join lateral (
            select c.duration_sec, c.page as web_event_page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.session_id in (6, 32)
        )
),
cleaned as (
    -- Basic cleansing and defaults
    select *, round(duration_sec * 1.07, 2) as adjusted_duration_sec
    from eligible
    where web_event_page is not null
)
-- Aggregate with conditional metrics
select
    browser,
    case when duration_sec >= 480 then 'LARGE' else 'SMALL' end as duration_sec_tier,
    round(avg(adjusted_duration_sec), 2) as avg_adjusted_duration_sec,
    count(*) as event_count
from cleaned
group by browser, case when duration_sec >= 480 then 'LARGE' else 'SMALL' end
order by browser
