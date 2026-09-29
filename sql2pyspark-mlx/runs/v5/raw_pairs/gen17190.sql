with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) e as attr_key, attr_value
),
parsed as (
    -- Higher-order array functions
    select
        id,
        log_ts,
        status_code,
        attr_key,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 2) != 0 as int) as is_flagged
    from flattened
),
scored as (
    -- Running and moving aggregates
    select
        *,
        min(is_flagged) over (partition by attr_key) as attr_key_min_is_flagged,
        sum(is_flagged) over (partition by attr_key order by cast(log_ts as bigint) range between 3600 preceding and current row) as is_flagged_sum_1h,
        row_number() over (order by log_ts, id, attr_key) as seq_no,
        last_value(is_flagged, true) over (partition by attr_key order by is_flagged, id, attr_key rows between unbounded preceding and current row) as last_known_is_flagged,
        dense_rank() over (partition by attr_key order by status_code_high desc nulls last) as status_code_high_dense_rank
    from parsed
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by attr_key order by is_flagged_sum_1h desc) as is_flagged_sum_1h_rank,
        sum(last_known_is_flagged) over (partition by attr_key order by is_flagged, id, attr_key rows between unbounded preceding and current row) as cumulative_last_known_is_flagged,
        lag(attr_key_min_is_flagged, 1, 0) over (order by status_code, id, attr_key) as prev_attr_key_min_is_flagged,
        last_value(last_known_is_flagged) ignore nulls over (partition by attr_key order by is_flagged, id, attr_key rows between unbounded preceding and current row) as last_known_last_known_is_flagged
    from scored
),
filtered as (
    -- Basic cleansing and defaults
    select *, round(status_code_high, 1) as status_code_high_rounded
    from metrics
    where log_ts > timestamp '2025-06-01 16:00:00'
      and attr_key is not null
),
prev_events as (
    -- Previous event time per key
    select *, lag(log_ts) over (partition by attr_key order by log_ts, id, attr_key) as prev_log_ts
    from filtered
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_log_ts is null or log_ts > prev_log_ts + interval 45 minutes then 1 else 0 end as new_group_flag
    from prev_events
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        log_ts,
        attr_key,
        attr_value,
        status_code_high,
        is_flagged_sum_1h,
        seq_no,
        last_known_is_flagged,
        status_code_high_dense_rank,
        is_flagged_sum_1h_rank,
        cumulative_last_known_is_flagged,
        prev_attr_key_min_is_flagged,
        status_code_high_rounded,
        sum(new_group_flag) over (partition by attr_key order by log_ts, id, attr_key rows between unbounded preceding and current row) as island_id
    from gaps
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        min(is_flagged_sum_1h_rank) over (partition by attr_key order by island_id, id, attr_key rows between 2 preceding and current row) as last3_min_is_flagged_sum_1h_rank,
        max(is_flagged_sum_1h_rank) over (partition by attr_key order by island_id, id, attr_key rows between 2 preceding and current row) as last3_max_is_flagged_sum_1h_rank,
        count(is_flagged_sum_1h_rank) over (partition by attr_key order by island_id, id, attr_key rows between 2 preceding and current row) as last3_count_is_flagged_sum_1h_rank,
        dense_rank() over (partition by attr_key order by seq_no nulls first) as seq_no_dense_rank
    from islands
),
daily as (
    -- Grouped metrics
    select
        date_trunc('hour', log_ts) as hour_window,
        max(last3_count_is_flagged_sum_1h_rank) filter (where attr_key = 'trace') as trace_max_last3_count_is_flagged_sum_1h_rank,
        round(avg(last3_min_is_flagged_sum_1h_rank), 2) as avg_last3_min_is_flagged_sum_1h_rank,
        count(*) as num_rows,
        count_if(attr_key = 'region') as region_events,
        count_if(attr_key = 'version') as version_events,
        (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as span_minutes
    from enriched
    group by 1
)
select
    hour_window,
    region_events,
    version_events,
    span_minutes
from daily
order by span_minutes, hour_window
