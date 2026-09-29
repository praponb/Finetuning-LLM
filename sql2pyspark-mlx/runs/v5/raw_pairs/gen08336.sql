with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        status_code,
        key,
        value
    from app_logs
    lateral view explode(context) e as key, value
),
unpacked as (
    -- Semi-structured parsing
    select
        id,
        log_ts,
        key,
        cast((status_code & 2) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from lines
    where key in ('trace', 'region')
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        log_ts,
        key,
        avg(is_flagged) over (partition by key order by status_code_high, id, key rows between 2 preceding and 1 preceding) as is_flagged_avg_2_prior,
        row_number() over (partition by key order by is_flagged, id, key) as seq_no,
        count(status_code_high) over (partition by key order by cast(log_ts as bigint) range between 7200 preceding and current row) as status_code_high_count_2h,
        avg(is_flagged) over (partition by key order by log_ts, id, key rows between 6 preceding and current row) as is_flagged_avg_6
    from unpacked
    where status_code_high >= 1
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        log_ts,
        key,
        is_flagged_avg_2_prior,
        seq_no,
        sum(is_flagged_avg_2_prior) over w_running as cumulative_is_flagged_avg_2_prior,
        dense_rank() over (partition by key order by status_code_high_count_2h nulls last) as status_code_high_count_2h_dense_rank
    from with_windows
    window w_running as (partition by key order by is_flagged_avg_6, id, key rows between unbounded preceding and current row)
),
with_windows_app_logs as (
    -- Ranking and gap features
    select
        id,
        log_ts,
        key,
        cumulative_is_flagged_avg_2_prior,
        lag(is_flagged_avg_2_prior) over (partition by key order by log_ts, id, key) as prev_is_flagged_avg_2_prior,
        lead(is_flagged_avg_2_prior) over (partition by key order by log_ts, id, key) as next_is_flagged_avg_2_prior,
        lead(cumulative_is_flagged_avg_2_prior) over (partition by key order by log_ts, id, key) as next_cumulative_is_flagged_avg_2_prior,
        row_number() over (partition by key order by log_ts, id, key) as row_seq,
        lead(seq_no, 2) over (partition by key order by log_ts, id, key) as next_seq_no
    from enriched
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        log_ts,
        key,
        cumulative_is_flagged_avg_2_prior,
        prev_is_flagged_avg_2_prior,
        next_is_flagged_avg_2_prior,
        next_cumulative_is_flagged_avg_2_prior,
        row_seq,
        next_seq_no,
        greatest(next_is_flagged_avg_2_prior, prev_is_flagged_avg_2_prior) as greatest_next_is_flagged_avg_2_prior_prev_is_flagged_avg_2_prior
    from with_windows_app_logs
    where next_seq_no between 3 and 8
      and log_ts >= date '2025-06-01'
),
with_windows_app_logs_2 as (
    -- Ranking and gap features
    select
        *,
        max(row_seq) over (partition by key order by row_seq, id, key rows between unbounded preceding and current row) as running_max_row_seq,
        count(row_seq) over (partition by key order by row_seq, id, key rows between unbounded preceding and current row) as running_count_row_seq,
        sum(row_seq) over (partition by key order by row_seq, id, key rows between unbounded preceding and current row) as running_sum_row_seq,
        lag(row_seq, 1) over (partition by key order by greatest_next_is_flagged_avg_2_prior_prev_is_flagged_avg_2_prior, id, key) as prev_row_seq
    from cleaned
)
select
    id,
    log_ts,
    key,
    cumulative_is_flagged_avg_2_prior,
    prev_is_flagged_avg_2_prior,
    next_is_flagged_avg_2_prior,
    next_cumulative_is_flagged_avg_2_prior,
    next_seq_no,
    greatest_next_is_flagged_avg_2_prior_prev_is_flagged_avg_2_prior
from with_windows_app_logs_2
order by log_ts nulls first;
