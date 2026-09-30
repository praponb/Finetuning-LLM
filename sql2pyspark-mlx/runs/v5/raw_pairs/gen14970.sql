with lines as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        v
    from app_logs
    lateral view explode(context) t as key, v
)
select
    date_trunc('hour', log_ts) as hour_window,
    bit_or(latency_ms) as latency_ms_bit_or,
    count(*) as cnt,
    sort_array(collect_set(service)) as service_list
from lines
group by date_trunc('hour', log_ts);
