with flattened as (
    select
        id,
        host,
        level,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view outer explode(context) e as attr_key, value
)
select
    id,
    level,
    status_code,
    value
from flattened
order by level desc nulls last
