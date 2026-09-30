with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        level,
        attr_key,
        v
    from app_logs
    lateral view explode(context) x as attr_key, v
)
select
    id,
    attr_key,
    v,
    case when attr_key rlike '^(trace|version)$' then 'PRIORITY' else 'OTHER_ZONE' end as attr_key_zone
from exploded
order by id nulls last;
