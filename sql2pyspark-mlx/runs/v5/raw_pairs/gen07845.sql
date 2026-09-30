with enriched as (
    -- Attach sessions attributes
    select
        src.*,
        s.device,
        s.country
    from web_events src
    inner join sessions s
        on src.session_id = s.id
    where s.browser not in ('Firefox', 'Edge')
),
base as (
    -- Keep recent, valid rows
    select
        *,
        concat(device, ' ', country) as device_country,
        substring(country, 1, 2) as country_prefix
    from enriched
    where country is not null
      and country <> 'TH'
),
enriched_web_events as (
    -- Attach sessions attributes
    select
        e.*,
        d.browser,
        d.country as session_country,
        d.device as session_device
    from base e
    inner join sessions d
        on e.session_id = d.id
),
matched_keys as (
    -- Set difference on keys
    select session_id
    from enriched_web_events
    except all
    select session_id
    from web_events
    where event_type <> 'search'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_web_events s
    inner join matched_keys k on s.session_id = k.session_id
),
joined as (
    -- Attach sessions attributes
    select
        s.session_id,
        s.event_type,
        s.event_time,
        s.duration_sec,
        s.device,
        s.country,
        s.device_country,
        s.country_prefix,
        s.session_country,
        s.session_device,
        d.device as session_device_2,
        d.country as session_country_2,
        d.browser as session_browser
    from verified s
    inner join sessions d
        on s.session_id = d.id
),
remaining_keys as (
    -- Set intersection on keys
    select event_type
    from joined
    intersect all
    select event_type
    from web_events
    where page in ('/home', '/cart')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join remaining_keys k on s.event_type = k.event_type
),
metrics as (
    -- Running and moving aggregates
    select
        session_id,
        duration_sec,
        device,
        country,
        country_prefix,
        session_country,
        session_device,
        session_country_2,
        session_browser,
        cume_dist() over (partition by session_browser, session_device_2 order by duration_sec desc nulls first) as duration_sec_cume_dist,
        max(duration_sec) over (partition by session_browser) as session_browser_max_duration_sec
    from reconciled
)
select
    session_id,
    device,
    country,
    session_country,
    session_device,
    session_country_2,
    session_browser,
    session_browser_max_duration_sec,
    case when device rlike '^(desktop|mobile)$' then 'OTHER_ZONE' when device rlike '^(tablet)$' then 'GROUP_B' else 'GROUP_A' end as device_zone
from metrics
order by session_country
