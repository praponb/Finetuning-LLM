with exploded as (
    select
        id,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) e as attr_key, v
),
parsed as (
    select
        id,
        level,
        latency_ms,
        attr_key,
        v,
        cast((status_code & 4) != 0 as int) as flag_bit4,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
cleaned as (
    select *, length(level) as level_length
    from parsed
    where attr_key is distinct from 'region'
      and level != 'WARN'
),
valid as (
    select
        *,
        lower(attr_key) as attr_key_lower,
        cast(level_length as string) as level_length_num,
        coalesce(attr_key, 'DIRECT') as attr_key_filled
    from cleaned
    where attr_key is not null
      and attr_key is not distinct from 'region'
)
select
    id,
    level,
    latency_ms,
    attr_key,
    flag_bit4,
    attr_key_lower,
    level_length_num,
    case when level rlike '^(DEBUG|WARN)$' then 'CORE' else 'GROUP_A' end as level_zone
from valid
order by flag_bit4
