with cleaned as (
    -- Keep recent, valid rows
    select *, month(event_time) as event_time_month
    from web_events
    where event_time > timestamp '2023-07-31 12:00:00'
),
kept_keys as (
    -- Set difference on keys
    select page, event_type
    from cleaned
    except distinct
    select page, event_type
    from web_events
    where page in ('/cart', '/home')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join kept_keys k on s.page = k.page and s.event_type = k.event_type
),
joined as (
    -- Attach sessions attributes
    select
        s.*,
        p.country,
        p.device
    from verified s
    left join sessions p
        on s.session_id = p.id
    where p.browser = 'Safari'
),
joined_web_events as (
    -- Attach sessions attributes
    select
        s.session_id,
        s.event_type,
        s.page,
        s.event_time,
        s.duration_sec,
        s.device,
        p.device as session_device,
        p.country as session_country
    from joined s
    left join sessions p
        on s.session_id = p.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when page = '/checkout' then 1 else 0 end as is_checkout,
        coalesce(page, 'DIRECT') as page_filled
    from joined_web_events
    where event_time > timestamp '2023-05-11 23:00:00'
      and duration_sec between 144 and 250
),
joined_web_events_2 as (
    -- Attach sessions attributes
    select
        s.*,
        b.country,
        b.browser,
        b.device as session_device_2
    from filtered s
    join sessions b
        on s.session_id = b.id
)
select
    s.session_id,
    s.event_type,
    s.page,
    s.duration_sec,
    s.session_device,
    s.session_country,
    s.is_checkout,
    s.page_filled,
    s.country,
    (select sum(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as sum_web_event_duration_sec
from joined_web_events_2 s;
