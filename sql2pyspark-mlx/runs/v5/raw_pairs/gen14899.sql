with matched_keys as (
    -- Set intersection on keys
    select page, id
    from web_events
    intersect all
    select page, id
    from web_events
    where duration_sec <> 371
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from web_events s
    inner join matched_keys k on s.page = k.page and s.id = k.id
),
enriched as (
    -- Attach sessions attributes
    select
        s.session_id,
        s.event_type,
        s.event_time,
        d.device,
        d.country
    from reconciled s
    left join sessions d
        on s.session_id = d.id
    where d.device = 'desktop'
),
cleaned as (
    -- Keep recent, valid rows
    select
        event_type,
        event_time,
        device,
        length(device) device_length
    from enriched
    where device <> 'desktop'
      and device is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, round(device_length, 2) device_length_rounded
    from cleaned
    where event_time >= date '2023-04-19'
      and device_length >= 79
),
filtered_web_events as (
    -- Keep recent, valid rows
    select
        *,
        substring(device, 1, 3) device_prefix,
        substring(device, 1, 4) device_prefix_2
    from filtered
    where event_time > timestamp '2023-04-26 09:00:00'
      and device is not distinct from 'tablet'
),
cleaned_web_events as (
    -- Basic cleansing and defaults
    select
        *,
        lower(event_type) event_type_lower,
        coalesce(device, 'NONE') device_clean
    from filtered_web_events
    where device is not null
      and device is distinct from 'tablet'
)
select
    event_type,
    event_time,
    device_length_rounded,
    device_prefix,
    device_prefix_2,
    event_type_lower,
    case when event_type rlike '^(page_view|click)$' then 'GROUP_B' when event_type rlike '^(add_to_cart|purchase)$' then 'OTHER_ZONE' else 'GROUP_A' end as event_type_zone
from cleaned_web_events
