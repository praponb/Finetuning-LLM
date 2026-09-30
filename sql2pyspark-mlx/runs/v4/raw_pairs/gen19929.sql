with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) x as k, attr_value
),
with_windows as (
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        k,
        attr_value,
        corr(latency_ms, status_code) over (partition by k, host) as corr_latency_ms_status_code,
        min(status_code) over (partition by service) as service_min_status_code,
        max(status_code) over (partition by host) as host_max_status_code,
        percent_rank() over (order by status_code) as status_code_percent_rank
    from flattened
),
metrics as (
    select
        *,
        round(status_code / sum(status_code) over (partition by k, host), 4) as status_code_share,
        dense_rank() over (partition by host order by host_max_status_code desc) as host_max_status_code_dense_rank,
        sum(status_code) over (partition by k order by cast(log_ts as bigint) range between 7200 preceding and current row) as status_code_sum_2h,
        sum(status_code_percent_rank) over (partition by k order by cast(log_ts as bigint) range between 7200 preceding and current row) as status_code_percent_rank_sum_2h
    from with_windows
),
grouped as (
    select
        service,
        sort_array(collect_set(k)) as k_list,
        (unix_timestamp(max(log_ts)) - unix_timestamp(min(log_ts))) / 60 as duration_min
    from metrics
    group by service
)
select service, case when service rlike '^(billing|auth)$' then 'CORE' when service rlike '^(search|checkout)$' then 'EXTENDED' else 'GROUP_A' end as service_zone
from grouped
order by service_zone desc
