with exploded as (
    select
        id,
        host,
        log_ts,
        latency_ms,
        key,
        value
    from app_logs
    lateral view explode(context) li as key, value
)
select
    host,
    count(*) as num_rows,
    bit_and(latency_ms) as latency_ms_bit_and
from exploded
group by host
order by latency_ms_bit_and nulls last
