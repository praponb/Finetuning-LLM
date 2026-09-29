with valid as (
    -- Basic cleansing and defaults
    select
        id,
        page,
        event_time,
        duration_sec,
        date_format(event_time, 'yyyy-MM-dd') as event_time_label,
        coalesce(page, 'NONE') as resolved_page
    from web_events
    where page is not null
      and duration_sec > 235
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '14' day then 1 else 0 end as session_start_flag
    from valid
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id
    from flagged
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by page order by event_time nulls first, id) as row_num
        from sessionized
    ) ranked
    where row_num = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        sum(duration_sec) over w_hist as page_sum_duration_sec,
        stddev_samp(session_start_flag) over w_hist as page_stddev_samp_session_start_flag,
        ntile(10) over (order by duration_sec desc, id) as duration_sec_bucket,
        cume_dist() over w_part as duration_sec_cume_dist
    from deduped
    window w_hist as (partition by page), w_part as (order by duration_sec desc nulls first)
),
with_prev as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by page order by event_time, id) as prev_event_time
    from enriched
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval '30' day then 1 else 0 end as session_start_flag_2
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag_2) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from gaps
)
select
    id,
    page,
    duration_sec,
    event_time_label,
    session_id,
    row_num,
    page_sum_duration_sec,
    page_stddev_samp_session_start_flag,
    duration_sec_bucket
from islands
order by page desc, page_sum_duration_sec;
