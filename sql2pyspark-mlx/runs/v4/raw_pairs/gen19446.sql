with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        to_date(event_time) as event_time_date,
        coalesce(duration_sec, 0) as duration_sec_filled,
        coalesce(page, 'N/A') as resolved_page
    from web_events
    where event_time >= date '2023-05-07'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by page order by duration_sec_filled, id) as latest_rank
        from cleaned
    ) ranked
    where latest_rank = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from latest
    qualify row_number() over (partition by page order by duration_sec, id) = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by page, session_id order by duration_sec, id) as dedup_rank
        from ranked
    ) ranked
    where dedup_rank = 1
)
-- Native SQL PIVOT
select *
from (
    select
        session_id,
        page,
        event_type,
        duration_sec
    from deduped
) src
pivot (
    avg(duration_sec)
    for event_type in ('add_to_cart', 'search')
);
