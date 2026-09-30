with base as (
    -- Basic cleansing and defaults
    select
        *,
        to_date(started_at) as started_at_date,
        coalesce(browser, 'NONE') as resolved_browser
    from sessions
    where device = 'tablet'
),
prev_events as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by browser order by started_at, id) as prev_started_at
    from base
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 30 days then 1 else 0 end as new_group_flag
    from prev_events
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
-- Multi-level aggregation
select
    device,
    round(sum(new_group_flag) / nullif(count(case when country = 'UK' then 1 end), 0), 2) as new_group_flag_per_uk,
    min(started_at) as started_at
from sessions_sessions
group by device
order by new_group_flag_per_uk desc;
