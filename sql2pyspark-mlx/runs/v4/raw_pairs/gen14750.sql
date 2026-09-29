with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
),
metrics as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value,
        cume_dist() over (order by status_code desc) as status_code_cume_dist,
        sum(status_code) over (partition by key) as key_sum_status_code
    from flattened
),
with_windows as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value,
        status_code_cume_dist,
        key_sum_status_code,
        corr(latency_ms, status_code) over w_hist as corr_latency_ms_status_code,
        percent_rank() over (partition by key order by status_code_cume_dist desc nulls first) as status_code_cume_dist_percent_rank,
        count(status_code_cume_dist) over (partition by service) as service_count_status_code_cume_dist,
        rank() over (order by status_code asc) as status_code_rank
    from metrics
    window w_hist as (partition by service, host)
),
filtered as (
    select *, trim(service) as clean_service
    from with_windows
    where log_ts >= to_timestamp('2025-06-01 15:00:00')
      and key is not null
),
cleaned as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value,
        status_code_cume_dist,
        key_sum_status_code,
        corr_latency_ms_status_code,
        status_code_cume_dist_percent_rank,
        service_count_status_code_cume_dist,
        clean_service,
        concat(level, ', ', host) as level_host
    from filtered
    where level = 'INFO'
),
metrics_app_logs as (
    select
        *,
        percent_rank() over (order by status_code asc) as status_code_percent_rank,
        cume_dist() over w_recent as service_count_status_code_cume_dist_cume_dist,
        corr(latency_ms, status_code) over w_running as corr_latency_ms_status_code_2
    from cleaned
    window w_recent as (partition by level order by service_count_status_code_cume_dist desc), w_running as (partition by service)
),
with_windows_app_logs as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value,
        status_code_cume_dist,
        key_sum_status_code,
        corr_latency_ms_status_code,
        status_code_cume_dist_percent_rank,
        service_count_status_code_cume_dist,
        clean_service,
        level_host,
        status_code_percent_rank,
        service_count_status_code_cume_dist_cume_dist,
        corr_latency_ms_status_code_2,
        avg(latency_ms) over (partition by service order by cast(log_ts as bigint) asc range between 3600 preceding and current row) as latency_ms_avg_1h,
        max(service_count_status_code_cume_dist_cume_dist) over w_part as service_count_status_code_cume_dist_cume_dist_max_2h
    from metrics_app_logs
    window w_part as (partition by key order by cast(log_ts as bigint) asc range between 7200 preceding and current row)
)
select
    date_trunc('day', log_ts) as activity_day,
    approx_percentile(service_count_status_code_cume_dist_cume_dist_max_2h, 0.9) as service_count_status_code_cume_dist_cume_dist_max_2h_p90,
    round(sum(corr_latency_ms_status_code_2) / nullif(count(case when host = 'h1' then 1 end), 0), 2) as corr_latency_ms_status_code_2_per_h1,
    (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as duration_min
from with_windows_app_logs
group by 1
having corr_latency_ms_status_code_2_per_h1 > 0;
