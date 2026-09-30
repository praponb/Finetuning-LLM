with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(page) as page_length,
        lower(event_type) as event_type_lower
    from web_events
    where event_type in ('search', 'purchase')
      and event_time > date '2023-09-15'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        event_type,
        duration_sec,
        event_type_lower,
        max(duration_sec) over (partition by event_type order by page_length, id rows between 3 preceding and current row) as last4_max_duration_sec,
        min(duration_sec) over (partition by event_type order by page_length, id rows between 3 preceding and current row) as last4_min_duration_sec,
        avg(duration_sec) over (partition by event_type order by page_length, id rows between 3 preceding and current row) as last4_avg_duration_sec,
        count(duration_sec) over (partition by event_type order by page_length, id rows between 3 preceding and current row) as last4_count_duration_sec
    from base
),
base_web_events as (
    -- Basic cleansing and defaults
    select
        id,
        event_type,
        duration_sec,
        last4_max_duration_sec,
        last4_min_duration_sec,
        last4_avg_duration_sec,
        last4_count_duration_sec,
        cast(last4_min_duration_sec as int) as last4_min_duration_sec_int
    from with_windows
    where last4_avg_duration_sec between 320 and 370
),
valid as (
    -- Keep recent, valid rows
    select *, last4_count_duration_sec - last4_max_duration_sec as last4_count_duration_sec_minus_last4_max_duration_sec
    from base_web_events
    where last4_min_duration_sec_int = 57
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        last4_count_duration_sec,
        last4_min_duration_sec_int
    from (
        select *, row_number() over (partition by event_type order by last4_count_duration_sec desc nulls first, id) as row_num
        from valid
    ) x
    where row_num = 1
)
-- Multi-level aggregation
select
    case when last4_min_duration_sec_int >= 89 then 'PREMIUM' else 'STANDARD' end as last4_min_duration_sec_int_tier,
    count(distinct id) as distinct_ids,
    round(avg(last4_min_duration_sec_int), 2) as avg_last4_min_duration_sec_int,
    round(avg(last4_min_duration_sec_int), 2) as avg_last4_min_duration_sec_int_2
from latest
group by last4_min_duration_sec_int_tier
order by distinct_ids desc nulls first
