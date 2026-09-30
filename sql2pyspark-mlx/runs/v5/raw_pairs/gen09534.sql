with scored as (
    -- Running and moving aggregates
    select
        id,
        session_id,
        event_type,
        page,
        max(duration_sec) over w_running last3_max_duration_sec,
        avg(duration_sec) over w_running last3_avg_duration_sec,
        count(duration_sec) over w_running last3_count_duration_sec,
        sum(duration_sec) over w_running last3_sum_duration_sec
    from web_events
    where page = '/checkout'
    window w_running as (partition by session_id order by event_time, id rows between 2 preceding and current row)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        event_type,
        page,
        last3_max_duration_sec,
        last3_avg_duration_sec,
        last3_count_duration_sec,
        last3_sum_duration_sec,
        case when last3_avg_duration_sec >= 420 then 'High' when last3_avg_duration_sec >= 210 then 'Medium' else 'Low' end last3_avg_duration_sec_level,
        cast(last3_sum_duration_sec as int) last3_sum_duration_sec_int,
        coalesce(page, 'UNKNOWN') page_filled
    from scored
    where last3_sum_duration_sec >= 2000
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        event_type,
        page,
        last3_count_duration_sec,
        last3_avg_duration_sec_level,
        page_filled,
        percent_rank() over (order by last3_count_duration_sec desc) last3_count_duration_sec_percent_rank,
        sum(last3_sum_duration_sec) over (partition by page order by last3_sum_duration_sec, id rows between unbounded preceding and current row) cumulative_last3_sum_duration_sec,
        avg(last3_sum_duration_sec) over (partition by page, event_type order by last3_count_duration_sec, id rows between 2 preceding and current row) last3_sum_duration_sec_avg_2
    from cleaned
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by event_type order by last3_count_duration_sec, id) as latest_rank
        from with_windows
    ) x
    where latest_rank = 1
)
select
    id,
    event_type,
    last3_count_duration_sec,
    last3_avg_duration_sec_level,
    last3_count_duration_sec_percent_rank,
    cumulative_last3_sum_duration_sec,
    latest_rank
from deduped
order by last3_count_duration_sec_percent_rank desc nulls last;
