with lines as (
    select
        id,
        service,
        log_ts,
        level,
        key,
        value
    from app_logs
    lateral view explode(context) li as key, value
)
select
    id,
    service,
    log_ts,
    level,
    key,
    value,
    case when level rlike '^(DEBUG|WARN)$' then 'GROUP_B' else 'OTHER_ZONE' end as level_zone
from lines
