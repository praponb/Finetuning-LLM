with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) li as attr_key, attr_value
)
select
    id,
    service,
    level,
    latency_ms,
    status_code,
    attr_key,
    case when level rlike '^(INFO|ERROR)$' then 'GROUP_A' when level rlike '^(WARN|DEBUG)$' then 'GROUP_B' else 'EXTENDED' end as level_zone
from flattened
order by level desc, attr_key asc
