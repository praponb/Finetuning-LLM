with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        k,
        v
    from app_logs
    lateral view explode(context) e as k, v
)
select
    k,
    count(*) filter (where level = 'ERROR') as error_rows,
    count(case when level = 'DEBUG' then 1 end) as debug_count,
    max(log_ts) as last_log_ts,
    grouping(k) as grouping_id
from flattened
group by rollup (k)
having count(case when level = 'DEBUG' then 1 end) > 1
order by k desc;
