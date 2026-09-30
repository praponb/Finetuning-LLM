with flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        host,
        level,
        payload_json,
        context,
        context['region'] as region,
        context['trace'] as trace
    from app_logs
)
-- Multi-level aggregation
select
    level,
    count(distinct region) as distinct_regions,
    count(*) as cnt,
    size(collect_set(level)) as num_levels,
    size(collect_set(level)) as num_levels_2
from flattened_attrs
group by level
having count(*) >= 1
