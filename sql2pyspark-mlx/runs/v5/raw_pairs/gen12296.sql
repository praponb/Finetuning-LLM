with exploded as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) e as attr_key, value
)
select
    *,
    round(status_code / sum(status_code) over (partition by level), 4) as status_code_share,
    rank() over (partition by service order by status_code desc) as status_code_rank
from exploded;
