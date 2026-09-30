with base as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        length(event_type) as event_type_length
    from web_events
    where page is not null
      and page is not distinct from '/home'
),
kept_keys as (
    -- Set intersection on keys
    select event_type, page
    from base
    intersect
    select event_type, page
    from web_events
    where duration_sec < 332
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.event_type = k.event_type and s.page = k.page
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        case when page = '/checkout' then 1 else 0 end as is_checkout,
        duration_sec - event_type_length as duration_sec_minus_event_type_length
    from reconciled
    where duration_sec < 292
),
kept_keys_2 as (
    -- Set difference on keys
    select event_type, page
    from cleaned
    except distinct
    select event_type, page
    from web_events
    where page in ('/home', '/search')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys_2 k on s.event_type = k.event_type and s.page = k.page
),
metrics as (
    -- Ranking and gap features
    select
        id,
        session_id,
        duration_sec,
        sum(duration_sec) over (partition by event_type) as event_type_sum_duration_sec,
        sum(event_type_length) over (partition by event_type) as event_type_sum_event_type_length
    from kept_rows
)
-- Grouped metrics
select
    session_id,
    case when duration_sec >= 500 then 'LARGE' else 'SMALL' end as duration_sec_tier,
    case when event_type_sum_event_type_length >= 443 then 'HIGH' else 'LOW' end as event_type_sum_event_type_length_tier,
    sum(event_type_sum_duration_sec) as total_event_type_sum_duration_sec,
    count(distinct session_id) as distinct_session_ids,
    bit_or(duration_sec) as duration_sec_bit_or
from metrics
group by session_id, case when duration_sec >= 500 then 'LARGE' else 'SMALL' end, case when event_type_sum_event_type_length >= 443 then 'HIGH' else 'LOW' end
order by total_event_type_sum_duration_sec desc nulls first;
