with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        lower(page) page_lower,
        coalesce(page, 'UNKNOWN') page_clean
    from web_events
    where event_time >= timestamp '2023-04-20 20:00:00'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        ntile(10) over (partition by page order by duration_sec, id) duration_sec_bucket,
        avg(duration_sec) over (order by duration_sec, id rows between 2 preceding and current row) duration_sec_avg_2
    from filtered
    where event_time >= '2024-05-26'
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        session_id,
        event_time,
        duration_sec,
        page_lower,
        duration_sec_bucket,
        duration_sec_avg_2,
        sum(duration_sec) over (partition by page order by duration_sec_bucket, id rows between unbounded preceding and current row) cumulative_duration_sec,
        avg(duration_sec_avg_2) over (partition by session_id) session_id_avg_duration_sec_avg_2
    from with_windows
),
joined as (
    -- Attach sessions attributes
    select s.*, p2.country
    from scored s
    join sessions p2
        on s.session_id = p2.id
),
enriched as (
    -- Attach sessions attributes
    select e.*, b.device
    from joined e
    left join sessions b
        on e.session_id = b.id
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by session_id order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by session_id order by event_time, id) + interval '90' day then 1 else 0 end as is_new_session
    from enriched
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        session_id,
        event_time,
        duration_sec_bucket,
        session_id_avg_duration_sec_avg_2,
        sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
cleaned as (
    -- Keep recent, valid rows
    select *, quarter(event_time) event_time_quarter
    from islands
    where event_time >= to_date('2023-04-13')
)
select
    s.id,
    s.session_id,
    s.duration_sec_bucket,
    s.burst_id,
    s.event_time_quarter,
    (select max(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as max_web_event_duration_sec
from cleaned s
order by burst_id nulls first
