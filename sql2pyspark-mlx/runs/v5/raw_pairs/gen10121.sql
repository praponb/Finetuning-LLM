with valid as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        upper(page) as page_upper
    from web_events
    where event_time > to_timestamp('2023-10-27 11:00:00')
      and page is not null
)
-- Grouped metrics
select
    page,
    min(duration_sec) as min_duration_sec,
    percentile_approx(duration_sec, array(0.25, 0.5, 0.75)) as duration_sec_quartiles,
    count(distinct id) as distinct_ids
from valid
group by page
