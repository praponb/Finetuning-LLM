with lines as (
    select
        id,
        host,
        status_code,
        key,
        value
    from app_logs
    lateral view explode(context) li as key, value
)
select
    id,
    key,
    percent_rank() over w_recent status_code_percent_rank,
    rank() over (partition by host order by status_code desc) status_code_rank,
    lead(status_code, 2) over (partition by host, key order by status_code asc, id asc, key asc) next_status_code
from lines
window w_recent as (order by status_code desc);
