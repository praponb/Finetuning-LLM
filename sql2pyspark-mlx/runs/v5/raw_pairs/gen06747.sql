with cleaned as (
    -- Basic cleansing and defaults
    select *, case when country in ('US', 'SG') then 'Priority' else 'Other' end as country_group
    from sessions
    where started_at > timestamp '2023-03-08 07:00:00'
      and to_date(started_at) >= '2025-04-27'
),
with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from cleaned src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.started_at >= sub.start_date
       and src.started_at < sub.end_date + interval '30' day
),
with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by plan order by started_at, id, subscription_id) as prev_started_at
    from with_window
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 7 days then 1 else 0 end as session_start_flag
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by plan order by started_at, id, subscription_id rows between unbounded preceding and current row) as session_no
    from flagged
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by device order by started_at, id, subscription_id) > interval 90 days then 1 else 0 end as is_new_session
    from sessionized
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        device,
        browser,
        started_at,
        subscription_id,
        prev_started_at,
        session_no,
        sum(is_new_session) over (partition by device order by started_at, id, subscription_id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from islands s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time <= s.prev_started_at
        )
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.started_at,
        s.subscription_id,
        s.session_no,
        latest.event_time,
        latest.page
    from eligible s
    left join lateral (
            select c.event_time, c.page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(session_no) over (partition by page order by started_at, id, subscription_id rows between 2 preceding and current row) as last3_avg_session_no,
        min(session_no) over (partition by page order by started_at, id, subscription_id rows between 2 preceding and current row) as last3_min_session_no,
        count(session_no) over (partition by page order by started_at, id, subscription_id rows between 2 preceding and current row) as last3_count_session_no,
        ntile(4) over (order by session_no desc, id, subscription_id) as session_no_bucket
    from latest_match
)
select
    id,
    started_at,
    subscription_id,
    session_no,
    page,
    last3_min_session_no,
    last3_count_session_no,
    session_no_bucket
from with_windows
order by started_at desc, page
