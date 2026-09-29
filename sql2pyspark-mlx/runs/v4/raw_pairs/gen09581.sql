with lines as (
    select
        id,
        service,
        log_ts,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view outer explode(context) e as attr_key, v
)
select
    *,
    count(status_code) over (order by cast(log_ts as bigint) asc range between 3600 preceding and current row) as status_code_count_1h,
    max(status_code) over (partition by service order by cast(log_ts as bigint) asc range between 900 preceding and current row) as status_code_max_15m,
    cume_dist() over (partition by service order by status_code asc) as status_code_cume_dist,
    stddev_samp(status_code) over (partition by attr_key) as attr_key_stddev_samp_status_code
from lines
