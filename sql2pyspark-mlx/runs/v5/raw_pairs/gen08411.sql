with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        cast(event_time as date) as event_time_day
    from web_events
    where event_time >= date '2023-08-14'
      and page is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        event_type,
        page,
        last_value(page, true) over (partition by page order by event_time, id rows between unbounded preceding and current row) as last_known_page,
        last_value(page) ignore nulls over (order by event_time_day, id rows between unbounded preceding and current row) as last_known_page_2,
        row_number() over (order by event_time_day, id) as seq_no,
        row_number() over (partition by event_type order by event_time desc, id) as seq_no_2,
        row_number() over (partition by event_type order by event_time desc, id) as seq_no_3
    from cleaned
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by event_type, page order by seq_no, id) as rn
    from scored
    qualify rn = 1
),
base as (
    -- Basic cleansing and defaults
    select *, seq_no - seq_no_3 as seq_no_minus_seq_no_3
    from latest
    where page is not null
)
select
    id,
    event_type,
    last_known_page,
    last_known_page_2,
    seq_no_2,
    rn,
    seq_no_minus_seq_no_3,
    case when event_type rlike '^(click|page_view)$' then 'EXTENDED' else 'PRIORITY' end as event_type_zone
from base
