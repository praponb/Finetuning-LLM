with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) e as key, value
),
flattened_attrs as (
    -- Semi-structured parsing
    select
        service,
        host,
        log_ts,
        status_code,
        key,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as flag_bit1
    from exploded
    where host in ('h2', 'h3', 'h4')
),
with_windows as (
    -- Ranking and gap features
    select
        service,
        host,
        log_ts,
        status_code,
        key,
        status_code_high,
        flag_bit1,
        count(flag_bit1) over w_recent as key_count_flag_bit1,
        round(status_code / sum(status_code) over (partition by host, key), 4) as status_code_share,
        stddev_samp(status_code_high) over w_recent as key_stddev_samp_status_code_high,
        count(status_code) over (order by cast(log_ts as bigint) range between 3600 preceding and current row) as status_code_count_1h
    from flattened_attrs
    window w_recent as (partition by key)
)
-- Running and moving aggregates
select
    service,
    host,
    log_ts,
    status_code,
    key,
    status_code_high,
    flag_bit1,
    key_count_flag_bit1,
    status_code_share,
    key_stddev_samp_status_code_high,
    status_code_count_1h,
    dense_rank() over (partition by key order by status_code_share desc nulls first) as status_code_share_dense_rank,
    count(status_code_share) over w_hist as service_count_status_code_share,
    count(status_code) over w_ordered as key_count_status_code,
    sum(status_code_high) over w_recent as status_code_high_sum_30m
from with_windows
window w_hist as (partition by service), w_ordered as (partition by key, service), w_recent as (order by cast(log_ts as bigint) range between 1800 preceding and current row)
