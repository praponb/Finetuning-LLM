with exploded as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) t as k, attr_value
)
select
    *,
    rank() over w_part as status_code_rank,
    count(status_code) over (partition by host order by cast(log_ts as bigint) range between 1800 preceding and current row) as status_code_count_30m,
    round(status_code / sum(status_code) over (partition by k), 4) as status_code_share
from exploded
window w_part as (partition by host order by status_code desc);
