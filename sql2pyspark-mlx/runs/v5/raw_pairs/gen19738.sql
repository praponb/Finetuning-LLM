with filtered as (
    -- Keep recent, valid rows
    select
        id,
        event_time,
        duration_sec,
        concat_ws('-', page, event_type) as page_event_type_key,
        substring(page, 1, 2) as page_prefix
    from web_events
    where page is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lag(duration_sec, 1) over (order by event_time, id) as prev_duration_sec,
        lag(duration_sec, 2) over (order by event_time, id) as prev_duration_sec_2
    from filtered
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, duration_sec * prev_duration_sec_2 as duration_sec_times_prev_duration_sec_2
    from with_windows
    where duration_sec > 219
),
base as (
    -- Keep recent, valid rows
    select
        id,
        event_time,
        duration_sec,
        page_event_type_key,
        page_prefix,
        prev_duration_sec,
        prev_duration_sec_2,
        date_trunc('MONTH', event_time) as event_time_period,
        least(duration_sec, prev_duration_sec) as least_duration_sec_prev_duration_sec
    from cleaned
    where event_time > timestamp '2023-03-31 19:00:00'
),
with_windows_web_events as (
    -- Window metrics over several different partitions
    select
        *,
        lead(prev_duration_sec) over w_hist as next_prev_duration_sec,
        lag(prev_duration_sec) over w_hist as prev_prev_duration_sec,
        lag(least_duration_sec_prev_duration_sec) over w_hist as prev_least_duration_sec_prev_duration_sec,
        row_number() over w_hist as event_seq,
        sum(least_duration_sec_prev_duration_sec) over w_hist_2 as running_least_duration_sec_prev_duration_sec
    from base
    where least_duration_sec_prev_duration_sec between 29 and 77
    window w_hist as (order by least_duration_sec_prev_duration_sec, id), w_hist_2 as (order by least_duration_sec_prev_duration_sec, id rows between unbounded preceding and current row)
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    measure,
    metric_value
from (
    select
        id,
        prev_least_duration_sec_prev_duration_sec,
        least_duration_sec_prev_duration_sec
    from with_windows_web_events
) src
unpivot (
    metric_value for measure in (prev_least_duration_sec_prev_duration_sec, least_duration_sec_prev_duration_sec)
);
