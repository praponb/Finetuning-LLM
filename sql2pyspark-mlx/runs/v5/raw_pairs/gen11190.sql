with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        cast(event_time as date) as event_time_day
    from web_events
    where page is not distinct from '/product'
      and to_date(event_time) < '2023-10-01'
),
latest as (
    -- QUALIFY-style window filter
    select *
    from filtered
    qualify rank() over (partition by session_id, page order by event_time_day) = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(5) over (partition by session_id, page order by duration_sec, id) as duration_sec_bucket,
        dense_rank() over w_hist as duration_sec_dense_rank,
        sum(duration_sec) over (partition by event_type) as event_type_sum_duration_sec,
        last_value(duration_sec) ignore nulls over w_hist_2 as last_known_duration_sec,
        percent_rank() over (partition by page order by duration_sec nulls first) as duration_sec_percent_rank
    from latest
    where session_id in (32, 38)
    window w_hist as (order by duration_sec desc), w_hist_2 as (partition by page, session_id order by event_time_day, id rows between unbounded preceding and current row)
),
with_dims as (
    -- Attach sessions attributes
    select
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.duration_sec,
        s.last_known_duration_sec,
        s.duration_sec_percent_rank,
        b.browser,
        b.country,
        b.device
    from with_windows s
    left join sessions b
        on s.session_id = b.id
)
select
    s.id,
    s.session_id,
    s.last_known_duration_sec,
    s.duration_sec_percent_rank,
    s.browser,
    s.country,
    s.device,
    (select avg(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as avg_web_event_duration_sec
from with_dims s
